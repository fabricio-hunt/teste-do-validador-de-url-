# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Orquestrador Principal: Validador de URLs VTEX
# MAGIC
# MAGIC **Projeto:** Validador de URLs VTEX (Bemol)
# MAGIC **Origem:** `bemolonline.backup_conteudo.agente_seo_db`
# MAGIC **Destino (log):** `bemolonline.backup_conteudo.backup_de_validacao_de_url`
# MAGIC **Volume por execução:** 50 SKUs (fase de observação inicial)
# MAGIC
# MAGIC ## Regras de negócio aplicadas (definidas e validadas nas sessões anteriores)
# MAGIC
# MAGIC | Condição | Ação |
# MAGIC |---|---|
# MAGIC | Produto **inativo** | Ignora — sem correção, sem redirect (`PULADO_INATIVO`) |
# MAGIC | Produto ativo, slug já correto | Ignora — idempotência (`PULADO_JA_CORRIGIDO`) |
# MAGIC | Produto ativo, slug incorreto, sem colisão | Verifica e remove redirect errôneo (se houver) → normaliza → PUT → redirect para a própria URL nova → `SUCESSO` |
# MAGIC | Produto ativo, slug incorreto, com colisão | Gera variação com sufixo do SKU → revalida → segue o fluxo acima |
# MAGIC | Colisão também na variação com sufixo | `ERRO_COLISAO_IRRESOLVIVEL` — não tenta um 3º nome |
# MAGIC | Falha de API em qualquer etapa | Isola o erro, loga (`ERRO_API`/`ERRO_PUT`/`ERRO_REDIRECT`), segue para o próximo item |
# MAGIC | Item já processado com `SUCESSO` em execução anterior | Pula antes mesmo de chamar a API (idempotência simples) |
# MAGIC | PUT confirmado mas redirect falhou | `ERRO_REDIRECT` — tratado como **alerta de prioridade alta** (404 real já está no ar) |
# MAGIC | Produto ativo **em tratamento** (slug com erro) que já tinha um redirect apontando sua URL atual para outro destino (ex: resíduo de quando esteve inativo) | Remove esse redirect errôneo antes de seguir — produto ativo nunca deve estar sendo redirecionado para outro lugar. Registrado como observação dentro do próprio `SUCESSO` (campo `mensagem_erro`), sem status separado |
# MAGIC
# MAGIC ⚠️ **Importante sobre o escopo da regra de redirect errôneo:** essa verificação só roda
# MAGIC para produtos que já estão sendo tratados por terem slug inválido — não para todo
# MAGIC produto ativo da base. Verificar isso em todos os produtos ativos é um escopo maior,
# MAGIC fora desta fase de observação inicial.
# MAGIC
# MAGIC ## O que este notebook NÃO faz
# MAGIC - Não envia e-mail (isso é responsabilidade do notebook `03_email_report`)
# MAGIC - Não decide quantos SKUs processar por nenhuma lógica de IA — é sempre os próximos N
# MAGIC   ainda não corrigidos com sucesso, na ordem em que aparecem na tabela de origem

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importa a biblioteca de cliente VTEX (notebook 01)

# COMMAND ----------

# MAGIC %run ./01_vtex_client

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração da execução

# COMMAND ----------

import uuid
import time
from datetime import datetime, timezone, timedelta

TABELA_ORIGEM = "bemolonline.backup_conteudo.agente_seo_db"
TABELA_LOG    = "bemolonline.backup_conteudo.backup_de_validacao_de_url"

QTD_SKUS_POR_EXECUCAO = 150  # Lote de produção (agendado para a madrugada)

EXECUTION_ID = str(uuid.uuid4())

print(f"Execution ID desta rodada: {EXECUTION_ID}")
print(f"Tabela origem: {TABELA_ORIGEM}")
print(f"Tabela log:    {TABELA_LOG}")
print(f"Qtd. SKUs:     {QTD_SKUS_POR_EXECUCAO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seleciona os próximos N SKUs ainda não corrigidos com sucesso
# MAGIC
# MAGIC Idempotência: um SKU que já tem uma linha com `status = 'SUCESSO'` na tabela de log
# MAGIC não é selecionado de novo. SKUs que falharam anteriormente (qualquer status de erro)
# MAGIC **são** reconsiderados — isso permite que uma falha transitória (timeout, rate limit)
# MAGIC seja automaticamente re-tentada em uma execução futura, sem intervenção manual.

# COMMAND ----------

query_proximos_skus = f"""
SELECT origem.SKU_VTEX
FROM {TABELA_ORIGEM} AS origem
WHERE origem.SKU_VTEX IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM {TABELA_LOG} AS log
      WHERE log.sku_vtex = origem.SKU_VTEX
        AND log.status IN ('SUCESSO', 'PULADO_JA_CORRIGIDO', 'PULADO_INATIVO')
  )
LIMIT {QTD_SKUS_POR_EXECUCAO}
"""

df_proximos = spark.sql(query_proximos_skus)
lista_skus = [row["SKU_VTEX"] for row in df_proximos.collect()]

print(f"Total de SKUs selecionados para esta execução: {len(lista_skus)}")
if len(lista_skus) < QTD_SKUS_POR_EXECUCAO:
    print(f"⚠️ Selecionados menos de {QTD_SKUS_POR_EXECUCAO} — pode indicar que a maior parte da")
    print("   base já foi corrigida com sucesso, ou que a tabela de origem tem menos linhas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Login único para esta execução
# MAGIC
# MAGIC Para o volume de 50 itens, um único login no início é suficiente (não deve expirar
# MAGIC no meio do lote). Se o volume crescer significativamente no futuro, reavaliar.

# COMMAND ----------

TOKEN_VTEX = obter_token_vtex()
print("✅ Login realizado para esta execução.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de processamento — um item por vez, com isolamento de falha
# MAGIC
# MAGIC Cada SKU é processado dentro de seu próprio bloco de tratamento de erro. Uma falha em
# MAGIC um item nunca interrompe o processamento dos demais (resiliência por item, conforme
# MAGIC definido nas regras de negócio).

# COMMAND ----------

def _now_utc():
    return datetime.now(timezone.utc)


def processar_sku(sku_vtex: str) -> dict:
    """
    Processa um único SKU, do início ao fim, retornando um dict pronto para
    se tornar uma linha da tabela de log. Nunca lança exception para fora —
    qualquer erro inesperado é capturado e classificado como ERRO_API.
    """
    inicio = time.time()

    linha = {
        "execution_id": EXECUTION_ID,
        "timestamp": _now_utc(),
        "sku_vtex": str(sku_vtex),
        "product_id": None,
        "slug_original": None,
        "slug_normalizado": None,
        "slug_final": None,
        "houve_colisao": False,
        "product_id_conflitante": None,
        "is_active": None,
        "status": None,
        "put_confirmado": False,
        "redirect_confirmado": False,
        "redirect_destino": None,
        "mensagem_erro": None,
        "tempo_processamento_ms": None,
    }

    try:
        # 1) SkuId -> ProductId
        resultado_sku = buscar_product_id(sku_vtex)
        if resultado_sku["erro"]:
            linha["status"] = "ERRO_API"
            linha["mensagem_erro"] = f"Falha ao buscar ProductId: {resultado_sku.get('msg')}"
            return _finalizar(linha, inicio)

        product_id = resultado_sku["product_id"]
        linha["product_id"] = product_id

        # 2) ProductId -> LinkId + IsActive
        detalhes = buscar_detalhes_produto(product_id)
        if detalhes["erro"]:
            linha["status"] = "ERRO_API"
            linha["mensagem_erro"] = f"Falha ao buscar detalhes do produto: {detalhes.get('msg')}"
            return _finalizar(linha, inicio)

        linha["is_active"] = detalhes["is_active"]
        linha["slug_original"] = detalhes["link_id"]

        # 3) Regra de negócio: produto inativo é ignorado integralmente
        if not detalhes["is_active"]:
            linha["status"] = "PULADO_INATIVO"
            return _finalizar(linha, inicio)

        # 4) Normalização
        slug_atual = detalhes["link_id"]
        slug_normalizado = normalizar_slug(slug_atual)
        linha["slug_normalizado"] = slug_normalizado

        # 5) Idempotência: slug já está correto -> nada a fazer
        if slug_atual == slug_normalizado:
            linha["status"] = "PULADO_JA_CORRIGIDO"
            return _finalizar(linha, inicio)

        # 5b) Produto em tratamento (slug com erro): verifica se a URL atual
        # dele está sendo indevidamente redirecionada para outro destino
        # (ex: resíduo de quando o produto esteve inativo no passado).
        # Produto ATIVO nunca deve estar sendo redirecionado — se houver
        # esse redirect errôneo, removemos antes de seguir. Registrado como
        # observação dentro do SUCESSO final (não é um erro, não tem status
        # próprio), conforme decidido nas regras de negócio.
        observacoes = []
        path_atual = f"/{slug_atual.lower()}/p"
        resultado_redirect_existente = verificar_redirect_existente(TOKEN_VTEX, path_atual)

        if resultado_redirect_existente.get("erro"):
            # Falha ao verificar não impede o fluxo principal — registra como
            # observação e segue (não é razão para abortar a correção do slug).
            observacoes.append(
                f"Não foi possível verificar redirect existente para a URL atual: "
                f"{resultado_redirect_existente.get('msg')}"
            )
        elif resultado_redirect_existente.get("existe"):
            resultado_remocao = remover_redirect(TOKEN_VTEX, path_atual)
            if resultado_remocao.get("erro"):
                observacoes.append(
                    f"Redirect errôneo detectado (produto ativo redirecionando para outro "
                    f"destino), mas falha ao remover: {resultado_remocao.get('msg')}"
                )
            else:
                destino_antigo = (resultado_redirect_existente.get("raw") or {}).get("to")
                observacoes.append(
                    f"Redirect errôneo removido: produto ativo estava sendo redirecionado "
                    f"para '{destino_antigo}'."
                )

        # 6) Checagem de colisão
        resultado_colisao = checar_colisao_slug(slug_normalizado, product_id)
        if resultado_colisao["erro"]:
            _definir_erro(linha, "ERRO_API", f"Falha ao checar colisão: {resultado_colisao.get('msg')}", observacoes)
            return _finalizar(linha, inicio)

        slug_final = slug_normalizado

        if resultado_colisao["colisao"]:
            linha["houve_colisao"] = True
            linha["product_id_conflitante"] = resultado_colisao["product_id_conflitante"]

            # Estratégia de desambiguação: sufixo do próprio SKU (determinístico,
            # garantia de unicidade matemática, sem necessidade de IA)
            slug_com_sufixo = f"{slug_normalizado}-{sku_vtex}"
            resultado_colisao_2 = checar_colisao_slug(slug_com_sufixo, product_id)

            if resultado_colisao_2["erro"]:
                _definir_erro(
                    linha, "ERRO_API",
                    f"Falha ao checar colisão (variação com sufixo): {resultado_colisao_2.get('msg')}",
                    observacoes,
                )
                return _finalizar(linha, inicio)

            if resultado_colisao_2["colisao"]:
                # Não tenta um 3º nome — vira erro para revisão manual
                _definir_erro(
                    linha, "ERRO_COLISAO_IRRESOLVIVEL",
                    f"Slug normalizado e variação com sufixo de SKU colidiram. "
                    f"Conflito original: productId {resultado_colisao['product_id_conflitante']}.",
                    observacoes,
                )
                return _finalizar(linha, inicio)

            slug_final = slug_com_sufixo

        linha["slug_final"] = slug_final

        # 7) PUT — atualiza o LinkId de verdade
        resultado_put = atualizar_slug_produto(product_id, detalhes["raw"], slug_final)
        if resultado_put["erro"]:
            _definir_erro(linha, "ERRO_PUT", f"Falha no PUT: {resultado_put.get('msg')}", observacoes)
            return _finalizar(linha, inicio)

        linha["put_confirmado"] = True

        # 8) Redirect definitivo — produto ATIVO redireciona para a própria URL nova
        #    (regra confirmada: só usaríamos destino alternativo para produto inativo,
        #    mas produto inativo já foi filtrado no passo 3 e nunca chega aqui)
        path_destino = f"/{slug_final}/p"
        path_atual_exato = f"/{slug_atual}/p"
        path_atual_min = f"/{slug_atual.lower()}/p"
        linha["redirect_destino"] = path_destino

        redirects_a_criar = []
        if path_atual_exato != path_destino:
            redirects_a_criar.append(path_atual_exato)
        if path_atual_min != path_destino and path_atual_min != path_atual_exato:
            redirects_a_criar.append(path_atual_min)

        if not redirects_a_criar:
            linha["redirect_confirmado"] = True
            observacoes.append("Redirect ignorado: as URLs de origem e destino seriam as mesmas (apenas case diferente no slug original).")
        else:
            for path_origem in redirects_a_criar:
                resultado_redirect = criar_redirect(TOKEN_VTEX, path_origem, path_destino)

                if resultado_redirect["erro"]:
                    # ⚠️ CASO CRÍTICO: PUT já confirmado, mas redirect falhou.
                    _definir_erro(
                        linha, "ERRO_REDIRECT",
                        f"PUT confirmado, mas redirect falhou para {path_origem}: {resultado_redirect.get('msg')}",
                        observacoes,
                    )
                    return _finalizar(linha, inicio)
                
            linha["redirect_confirmado"] = True
        linha["status"] = "SUCESSO"
        if observacoes:
            linha["mensagem_erro"] = " | ".join(observacoes)
        return _finalizar(linha, inicio)

    except Exception as exc:
        # Rede de segurança final — nenhuma exception não-tratada deve
        # interromper o processamento dos demais itens do lote.
        linha["status"] = "ERRO_API"
        linha["mensagem_erro"] = f"Exceção não tratada: {type(exc).__name__}: {exc}"
        return _finalizar(linha, inicio)


def _finalizar(linha: dict, inicio: float) -> dict:
    linha["tempo_processamento_ms"] = int((time.time() - inicio) * 1000)
    return linha


def _definir_erro(linha: dict, status: str, mensagem: str, observacoes: list) -> dict:
    """
    Define o status de erro e a mensagem, sempre incluindo qualquer observação
    já coletada antes (ex: redirect errôneo removido) — para não perder esse
    contexto mesmo quando o item termina em erro mais adiante no fluxo.
    """
    linha["status"] = status
    partes = [mensagem] + observacoes
    linha["mensagem_erro"] = " | ".join(partes)
    return linha

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução do lote

# COMMAND ----------

resultados = []

for i, sku in enumerate(lista_skus, start=1):
    print(f"[{i}/{len(lista_skus)}] Processando SKU {sku}...")
    resultado = processar_sku(sku)
    resultados.append(resultado)
    print(f"    -> status: {resultado['status']} ({resultado['tempo_processamento_ms']} ms)")

print(f"\n✅ Processamento concluído: {len(resultados)} itens.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravação no log (Delta)
# MAGIC
# MAGIC ⚠️ Usamos um schema EXPLÍCITO (StructType), não inferência automática. Isso é
# MAGIC necessário porque colunas opcionais como `product_id_conflitante` podem vir
# MAGIC como `None` em TODAS as linhas de um lote (ex: nenhuma colisão na rodada) —
# MAGIC nesse caso, `spark.createDataFrame()` sem schema explícito falha com
# MAGIC `CANNOT_DETERMINE_TYPE`, pois não tem nenhum valor não-nulo para inferir o tipo.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    LongType, BooleanType, IntegerType,
)

SCHEMA_LOG = StructType([
    StructField("execution_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("sku_vtex", StringType(), nullable=False),
    StructField("product_id", LongType(), nullable=True),
    StructField("slug_original", StringType(), nullable=True),
    StructField("slug_normalizado", StringType(), nullable=True),
    StructField("slug_final", StringType(), nullable=True),
    StructField("houve_colisao", BooleanType(), nullable=True),
    StructField("product_id_conflitante", LongType(), nullable=True),
    StructField("is_active", BooleanType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("put_confirmado", BooleanType(), nullable=True),
    StructField("redirect_confirmado", BooleanType(), nullable=True),
    StructField("redirect_destino", StringType(), nullable=True),
    StructField("mensagem_erro", StringType(), nullable=True),
    StructField("tempo_processamento_ms", IntegerType(), nullable=True),
])

# COMMAND ----------

if resultados:
    df_resultados = spark.createDataFrame(resultados, schema=SCHEMA_LOG)
    df_resultados.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(TABELA_LOG)
    print(f"✅ {len(resultados)} linhas gravadas em {TABELA_LOG}.")
else:
    print("⚠️ Nenhum resultado para gravar (lista de SKUs estava vazia).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da execução (exibido no notebook + disponível para o e-mail)

# COMMAND ----------

from collections import Counter

contagem_status = Counter(r["status"] for r in resultados)

print("=" * 60)
print("RESUMO DA EXECUÇÃO")
print("=" * 60)
print(f"Execution ID: {EXECUTION_ID}")
print(f"Total processado: {len(resultados)}")
for status, qtd in contagem_status.most_common():
    print(f"  {status}: {qtd}")

erros_redirect = [r for r in resultados if r["status"] == "ERRO_REDIRECT"]
if erros_redirect:
    print("\n🚨 ALERTA DE PRIORIDADE ALTA — PUT confirmado mas redirect falhou:")
    for r in erros_redirect:
        print(f"   SKU {r['sku_vtex']} (ProductId {r['product_id']}): {r['mensagem_erro']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Disponibiliza o execution_id para o próximo notebook (e-mail) via Job Task Values
# MAGIC
# MAGIC Se este notebook for executado como uma task dentro de um Databricks Job (junto com
# MAGIC o notebook de e-mail como próxima task), o `execution_id` fica disponível para a task
# MAGIC seguinte sem precisar adivinhar/reconsultar qual foi a última execução.

# COMMAND ----------

try:
    dbutils.jobs.taskValues.set(key="execution_id", value=EXECUTION_ID)  # noqa: F821
    print(f"✅ execution_id ({EXECUTION_ID}) disponibilizado para a próxima task do Job.")
except Exception as e:
    print(f"(Não foi possível definir taskValues — normal se este notebook for rodado isoladamente: {e})")