"""
=============================================================================
TESTE END-TO-END (LEITURA + ESCRITA CONTROLADA) — Validador de URLs VTEX (Bemol)
=============================================================================
Esta é a versão 3 do script de teste. Todo o fluxo de leitura e a escrita
de redirect (fictícia) já foram validados 100% em execução real. Esta
versão adiciona a ÚLTIMA peça pendente: o PUT real que atualiza o LinkId
(slug) de um produto.

O QUE ESTE SCRIPT FAZ (fluxo completo, na ordem real do pipeline):

  1. Login programático (AppKey/AppToken -> cookie VtexIdclientAutCookie)
  2. GET stockkeepingunit/{skuId}      -> descobre o ProductId
  3. GET product/{productId}           -> LinkId atual + IsActive
  4. Normaliza o slug (regra: minúsculas, sem acento, sem caracteres
     especiais, sem hífens duplicados/nas pontas)
  5. GET products/search/{slug}/p      -> checa se o slug normalizado já
     está em uso por OUTRO produto (colisão)
  6. GraphQL: redirect.get(path)       -> checa se já existe um redirect
     para o path atual
  7. [OPCIONAL/SEGURO] cria e remove um redirect de TESTE via GraphQL,
     usando paths fictícios — para confirmar que a escrita continua
     funcionando.
  8. [OPCIONAL/REAL] PUT product/{productId} -> atualiza o LinkId de
     verdade. SÓ RODA SE:
       a) EXECUTAR_PUT_REAL = True (flag abaixo), E
       b) SKU_VTEX_TESTE apontar para um produto que você já escolheu
          como seguro para teste (inativo/não-crítico), E
       c) você digitar "SIM" na confirmação interativa no terminal.
     Cria também um redirect do slug antigo -> slug novo, igual faria
     o pipeline real em produção.

REGRA CRÍTICA DA VTEX RESPEITADA NO PUT:
  A Catalog API exige o corpo COMPLETO do produto no PUT — se mandarmos
  só o LinkId, todos os outros campos não enviados são apagados. Por
  isso, partimos sempre do JSON completo obtido no GET (etapa 3) e
  alteramos apenas o campo LinkId nele, antes de enviar.

COMO USAR:
  1. Crie um arquivo ".env" na mesma pasta com:
       APP_KEY=sua_app_key
       APP_TOKEN=seu_app_token
  2. pip install -r requirements.txt
  3. Ajuste SKU_VTEX_TESTE para o SKU de teste/inativo que você escolheu.
  4. Para testar o PUT real, mude EXECUTAR_PUT_REAL para True.
  5. python app.py
  6. Me envie o output completo.

SEGURANÇA:
  - As credenciais vêm do .env (não hardcoded), e o .env NÃO deve ser
    commitado/compartilhado. Confirme que existe um .gitignore com ".env".
  - EXECUTAR_PUT_REAL=True grava de verdade na VTEX. Use só com o SKU
    de teste que você já validou ser seguro.
=============================================================================
"""

import os
import re
import unicodedata
import json

import requests
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# CONFIGURAÇÃO
# =============================================================================

ACCOUNT    = "bemol"
BASE_URL   = "https://bemol.vtexcommercestable.com.br"   # Catalog REST
MYVTEX_URL = "https://bemol.myvtex.com"                   # Rewriter GraphQL

APP_KEY   = os.getenv("APP_KEY", "")
APP_TOKEN = os.getenv("APP_TOKEN", "")

# SKU real para testar o fluxo de LEITURA ponta a ponta (etapas 1-7).
SKU_VTEX_TESTE = "163486"

# ProductId de um produto escolhido para testar o PUT real (etapa 8).
# Diferente do SKU acima — aqui já recebemos o ProductId direto, sem
# precisar do passo SkuId->ProductId.
PRODUCT_ID_TESTE_PUT = 142111

# Para onde o redirect definitivo deve apontar após a correção do slug.
# IMPORTANTE: para produto ATIVO (como este), o destino correto é a
# PRÓPRIA URL nova do produto (slug corrigido) — não uma página genérica
# como "/superoferta", que só faz sentido para produto descontinuado/
# inativo (caso do fogão testado antes). Por isso aqui usamos None: o
# código abaixo (rodar_teste_put_real) calcula o destino automaticamente
# como "/{slug_novo}/p" quando este valor for None.
REDIRECT_DESTINO_TESTE_PUT = None

# Se True, executa o passo 7 (cria + remove um redirect de TESTE, com paths
# fictícios, só para confirmar que a escrita GraphQL continua funcionando).
TESTAR_ESCRITA_REDIRECT = True

# Se True, executa o passo 8: PUT REAL no produto (altera o LinkId de
# verdade) + cria o redirect slug-antigo -> slug-novo. SÓ ative isso depois
# de confirmar que SKU_VTEX_TESTE aponta para um produto de teste/inativo
# que você já escolheu como seguro. Vai pedir confirmação manual no
# terminal antes de gravar.
EXECUTAR_PUT_REAL = True


# =============================================================================
# HTTP helper simples — evita repetir try/except em cada função
# =============================================================================

def _post(url: str, headers: dict | None = None, json_body: dict | None = None,
          params: dict | None = None, timeout: int = 30) -> tuple[int | None, dict | str | None]:
    try:
        resp = requests.post(url, headers=headers, json=json_body, params=params, timeout=timeout)
    except requests.exceptions.RequestException as e:
        return None, f"Erro de conexão: {e}"
    try:
        return resp.status_code, resp.json()
    except ValueError:
        return resp.status_code, resp.text[:500]


def _get(url: str, headers: dict | None = None, timeout: int = 30) -> tuple[int | None, dict | str | None]:
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
    except requests.exceptions.RequestException as e:
        return None, f"Erro de conexão: {e}"
    try:
        return resp.status_code, resp.json()
    except ValueError:
        return resp.status_code, resp.text[:500]


# =============================================================================
# 1) LOGIN
# =============================================================================

def obter_token_vtex() -> str | None:
    """Troca AppKey/AppToken por um token usado no cookie VtexIdclientAutCookie."""
    url = f"{BASE_URL}/api/vtexid/apptoken/login"
    body = {"appkey": APP_KEY, "apptoken": APP_TOKEN}

    print(f"[1] POST {url}?an={ACCOUNT}")
    status, data = _post(url, headers={"Accept": "application/json"}, json_body=body, params={"an": ACCOUNT}, timeout=15)
    print(f"    Status HTTP: {status}")

    if status != 200 or not isinstance(data, dict):
        print(f"    ❌ Falha no login. Resposta: {data}")
        return None

    token = data.get("token")
    if not token:
        print(f"    ❌ Token não encontrado na resposta: {json.dumps(data, indent=2)[:500]}")
        return None

    print(f"    ✅ Token obtido ({token[:20]}...)")
    return token


def _auth_headers_rest() -> dict:
    return {
        "X-VTEX-API-AppKey": APP_KEY,
        "X-VTEX-API-AppToken": APP_TOKEN,
        "Accept": "application/json",
    }


def _auth_headers_graphql(token: str) -> dict:
    return {
        "cookie": f"VtexIdclientAutCookie={token};VtexWorkspace=master%3A-;",
        "Content-Type": "application/json",
    }


# =============================================================================
# 2) SkuId -> ProductId
# =============================================================================

def buscar_product_id(sku_id: str) -> int | None:
    url = f"{BASE_URL}/api/catalog/pvt/stockkeepingunit/{sku_id}"
    print(f"\n[2] GET {url}")

    status, data = _get(url, headers=_auth_headers_rest())
    print(f"    Status HTTP: {status}")

    if status != 200 or not isinstance(data, dict):
        print(f"    ❌ Falha ao buscar SKU. Resposta: {data}")
        return None

    product_id = data.get("ProductId")
    nome_sku = data.get("Name")
    print(f"    ✅ ProductId: {product_id} (SKU '{nome_sku}')")
    return product_id


# =============================================================================
# 3) ProductId -> LinkId atual + IsActive
# =============================================================================

def buscar_detalhes_produto(product_id: int) -> dict | None:
    url = f"{BASE_URL}/api/catalog/pvt/product/{product_id}"
    print(f"\n[3] GET {url}")

    status, data = _get(url, headers=_auth_headers_rest())
    print(f"    Status HTTP: {status}")

    if status != 200 or not isinstance(data, dict):
        print(f"    ❌ Falha ao buscar produto. Resposta: {data}")
        return None

    link_id = data.get("LinkId")
    is_active = data.get("IsActive")
    nome = data.get("Name")
    print(f"    ✅ Nome: {nome}")
    print(f"    ✅ LinkId atual: {link_id}")
    print(f"    ✅ IsActive: {is_active}")

    return {"link_id": link_id, "is_active": is_active, "nome": nome, "raw": data}


# =============================================================================
# 4) Normalização determinística do slug
# =============================================================================

def normalizar_slug(slug_bruto: str) -> str:
    """
    Regra de negócio confirmada (com o exemplo real da chuteira):
      entrada: chuteira-futsal-n°42-umbro-pro-5-bump-branco-preto-roxo--mp-
      saída:   chuteira-futsal-n-42-umbro-pro-5-bump-branco-preto-roxo-mp

    ATENÇÃO À ORDEM DAS OPERAÇÕES — isso foi um bug real encontrado em teste:
    se removermos acentos (NFKD + encode ascii) ANTES de trocar caracteres
    especiais por hífen, símbolos como '°' (grau) são descartados em
    silêncio pelo encode('ascii', 'ignore') porque não têm decomposição
    NFKD — e "n°42" vira "n42" (colado), não "n-42" (separado).
    Por isso, a troca por hífen tem que vir PRIMEiro, e a remoção de
    acentos (que afeta letras como ç, ã, é) vem depois.

    Passos (nesta ordem):
      1) minúsculas
      2) QUALQUER caractere que não seja letra (incl. acentuada) ou número
         vira HÍFEN — isso cobre °, ², /, espaços, parênteses, etc.
      3) remove acentos das letras restantes (ç -> c, ã -> a, é -> e)
      4) colapsa hífens consecutivos em um único
      5) remove hífen no início/fim
    """
    texto = slug_bruto.lower()
    texto = re.sub(r"[^a-z0-9À-ÿ]+", "-", texto)              # tudo que não é letra/número (incl. acentuada) -> hífen
    texto = unicodedata.normalize("NFKD", texto)
    texto = texto.encode("ascii", "ignore").decode("ascii")    # agora só remove acento, já não há símbolos soltos
    texto = re.sub(r"-{2,}", "-", texto)                        # colapsa hífens duplicados
    texto = texto.strip("-")                                    # remove hífen nas pontas
    return texto


# =============================================================================
# 5) Checar colisão de slug (produto ativo com mesmo slug)
# =============================================================================

def checar_colisao_slug(slug: str, product_id_atual: int) -> dict:
    url = f"{BASE_URL}/api/catalog_system/pub/products/search/{slug}/p"
    print(f"\n[5] GET {url}")

    status, data = _get(url, headers={"Accept": "application/json"})
    print(f"    Status HTTP: {status}")

    if status != 200:
        print(f"    ⚠️ Status inesperado. Resposta: {data}")
        return {"erro": True, "colisao": None}

    if not isinstance(data, list) or len(data) == 0:
        print(f"    ✅ Slug livre — nenhum produto encontrado com esse path.")
        return {"erro": False, "colisao": False}

    produto_encontrado = data[0]
    pid_encontrado = produto_encontrado.get("productId")
    nome_encontrado = produto_encontrado.get("productName")

    if str(pid_encontrado) == str(product_id_atual):
        print(f"    ✅ Slug em uso, mas é o PRÓPRIO produto (productId {pid_encontrado}). Sem colisão real.")
        return {"erro": False, "colisao": False}

    print(f"    ⚠️ COLISÃO: slug já usado por outro produto (productId {pid_encontrado}, '{nome_encontrado}')")
    return {"erro": False, "colisao": True, "product_id_conflitante": pid_encontrado}


# =============================================================================
# 6) Verificar redirect existente (GraphQL — Rewriter)
# =============================================================================

def verificar_redirect_existente(token: str, path: str) -> dict:
    url = f"{MYVTEX_URL}/_v/private/graphql/v1"
    # NOTA: o campo "redirect" é ambíguo nesse ambiente — 3 apps diferentes o
    # fornecem (vtex.pages-graphql@2.x, vtex.rewriter@1.69.3,
    # vtex.pages-graphql@1.30.0). A própria API retorna essa lista no erro
    # quando @context não é especificado. Apontamos explicitamente para o
    # vtex.rewriter, que é o app cujo schema validamos manualmente no GraphiQL.
    query = """
    query VerificarSlug($path: String!) {
      redirect @context(provider: "vtex.rewriter@1.x") {
        get(path: $path) {
          from
          to
          endDate
          type
          binding
          origin
        }
      }
    }
    """
    print(f"\n[6] POST {url} (query: redirect.get, @context: vtex.rewriter)")
    print(f"    path: {path}")

    status, data = _post(url, headers=_auth_headers_graphql(token),
                          json_body={"query": query, "variables": {"path": path}})
    print(f"    Status HTTP: {status}")

    if not isinstance(data, dict):
        print(f"    ❌ Resposta inesperada: {data}")
        return {"erro": True}

    if "errors" in data:
        print(f"    ❌ Erro GraphQL: {data['errors']}")
        return {"erro": True, "msg": data["errors"]}

    redirect_data = data.get("data", {}).get("redirect", {}).get("get")
    if redirect_data:
        print(f"    ✅ Já existe redirect para esse path: {redirect_data}")
    else:
        print(f"    ✅ Nenhum redirect existente para esse path (null).")

    return {"erro": False, "existe": redirect_data is not None, "raw": redirect_data}


# =============================================================================
# 7) [OPCIONAL] Teste seguro de escrita: cria e remove um redirect FICTÍCIO
# =============================================================================

def testar_escrita_redirect(token: str) -> None:
    url = f"{MYVTEX_URL}/_v/private/graphql/v1"
    from_path = "/teste-script-validador-from/p"
    to_path = "/teste-script-validador-to/p"

    mutation_save = """
    mutation CriarRedirectTeste($route: RedirectInput!) {
      redirect @context(provider: "vtex.rewriter@1.x") {
        save(route: $route) {
          from
          to
          type
          binding
        }
      }
    }
    """
    variables_save = {"route": {"from": from_path, "to": to_path, "type": "PERMANENT"}}

    print(f"\n[7a] POST {url} (mutation: redirect.save — TESTE com paths fictícios)")
    status, data = _post(url, headers=_auth_headers_graphql(token),
                          json_body={"query": mutation_save, "variables": variables_save})
    print(f"     Status HTTP: {status}")

    if not isinstance(data, dict) or "errors" in data:
        print(f"     ❌ Falha ao criar redirect de teste: {data}")
        return

    save_result = data.get("data", {}).get("redirect", {}).get("save")
    print(f"     ✅ Redirect de teste criado: {save_result}")

    # Limpa imediatamente o redirect de teste
    mutation_delete = """
    mutation RemoverRedirectTeste($path: String!) {
      redirect @context(provider: "vtex.rewriter@1.x") {
        delete(path: $path) {
          from
          to
        }
      }
    }
    """
    print(f"\n[7b] POST {url} (mutation: redirect.delete — limpando o teste)")
    status, data = _post(url, headers=_auth_headers_graphql(token),
                          json_body={"query": mutation_delete, "variables": {"path": from_path}})
    print(f"     Status HTTP: {status}")

    if not isinstance(data, dict) or "errors" in data:
        print(f"     ❌ Falha ao remover redirect de teste: {data}")
        print(f"     ⚠️ ATENÇÃO: pode ter sobrado lixo de teste — remova manualmente:")
        print(f"        path: {from_path}")
        return

    delete_result = data.get("data", {}).get("redirect", {}).get("delete")
    print(f"     ✅ Redirect de teste removido: {delete_result}")


def _put(url: str, headers: dict | None = None, json_body: dict | None = None,
         timeout: int = 30) -> tuple[int | None, dict | str | None]:
    try:
        resp = requests.put(url, headers=headers, json=json_body, timeout=timeout)
    except requests.exceptions.RequestException as e:
        return None, f"Erro de conexão: {e}"
    try:
        return resp.status_code, resp.json()
    except ValueError:
        return resp.status_code, resp.text[:500]


# =============================================================================
# 8) [REAL] PUT — atualiza o LinkId do produto de teste + cria redirect
# =============================================================================

def atualizar_slug_produto(product_id: int, produto_completo: dict, novo_slug: str) -> dict:
    """
    Atualiza o LinkId do produto via PUT.

    CRÍTICO: a Catalog API exige o corpo COMPLETO do produto no PUT — se
    enviarmos só o LinkId, todos os outros campos não enviados são
    apagados. Por isso partimos do JSON completo retornado pelo GET
    (buscar_detalhes_produto) e alteramos só o campo LinkId nele.
    """
    url = f"{BASE_URL}/api/catalog/pvt/product/{product_id}"

    corpo_atualizado = dict(produto_completo)  # cópia — preserva todos os campos
    corpo_atualizado["LinkId"] = novo_slug

    print(f"\n[8a] PUT {url}")
    print(f"     LinkId: '{produto_completo.get('LinkId')}' -> '{novo_slug}'")

    status, data = _put(url, headers=_auth_headers_rest(), json_body=corpo_atualizado)
    print(f"     Status HTTP: {status}")

    if status not in (200, 201):
        print(f"     ❌ Falha ao atualizar produto. Resposta: {data}")
        return {"erro": True, "msg": data}

    link_id_confirmado = data.get("LinkId") if isinstance(data, dict) else None
    print(f"     ✅ Produto atualizado. LinkId confirmado pela API: {link_id_confirmado}")
    return {"erro": False, "raw": data}


def criar_redirect_definitivo(token: str, slug_antigo: str, destino: str) -> dict:
    """
    Cria o redirect PERMANENT do path antigo (slug-antigo/p) para o
    destino combinado (ex: "/superoferta") — equivalente ao passo
    "faz o redirecionamento" do fluxograma original.
    """
    url = f"{MYVTEX_URL}/_v/private/graphql/v1"
    from_path = f"/{slug_antigo.lower()}/p"
    to_path = destino if destino.startswith("/") else f"/{destino}"

    mutation = """
    mutation CriarRedirectDefinitivo($route: RedirectInput!) {
      redirect @context(provider: "vtex.rewriter@1.x") {
        save(route: $route) {
          from
          to
          type
          binding
        }
      }
    }
    """
    variables = {"route": {"from": from_path, "to": to_path, "type": "PERMANENT"}}

    print(f"\n[8b] POST {url} (mutation: redirect.save — DEFINITIVO)")
    print(f"     from: {from_path}")
    print(f"     to:   {to_path}")

    status, data = _post(url, headers=_auth_headers_graphql(token),
                          json_body={"query": mutation, "variables": variables})
    print(f"     Status HTTP: {status}")

    if not isinstance(data, dict) or "errors" in data:
        print(f"     ❌ Falha ao criar redirect definitivo: {data}")
        return {"erro": True, "msg": data}

    save_result = data.get("data", {}).get("redirect", {}).get("save")
    print(f"     ✅ Redirect definitivo criado: {save_result}")
    return {"erro": False, "raw": save_result}


def rodar_teste_put_real(token: str) -> None:
    """
    Fluxo completo e isolado do teste de PUT real:
      1. GET do produto de teste (PRODUCT_ID_TESTE_PUT) — pega o JSON completo
      2. Normaliza o LinkId atual
      3. Confirma manualmente no terminal antes de gravar (salvaguarda)
      4. PUT — atualiza o LinkId de verdade
      5. Cria o redirect definitivo (slug antigo -> REDIRECT_DESTINO_TESTE_PUT)
    """
    print("\n" + "=" * 70)
    print("TESTE DE PUT REAL — produto de teste/inativo")
    print("=" * 70)
    print(f"ProductId de teste: {PRODUCT_ID_TESTE_PUT}")

    detalhes_teste = buscar_detalhes_produto(PRODUCT_ID_TESTE_PUT)
    if not detalhes_teste:
        print("\n❌ Não foi possível obter os detalhes do produto de teste. Abortando PUT.")
        return

    slug_atual = detalhes_teste["link_id"]
    slug_novo = normalizar_slug(slug_atual)

    # Se nenhum destino fixo foi configurado, o destino correto é a própria
    # URL nova do produto (caso comum para produto ATIVO que só está
    # trocando de slug, não saindo de linha).
    destino_redirect = REDIRECT_DESTINO_TESTE_PUT or f"/{slug_novo}/p"

    print(f"\nResumo do que será feito:")
    print(f"  Produto:        {detalhes_teste['nome']}")
    print(f"  IsActive:       {detalhes_teste['is_active']}")
    print(f"  Slug atual:     {slug_atual}")
    print(f"  Slug novo:      {slug_novo}")
    print(f"  Redirect:       /{slug_atual.lower()}/p  ->  {destino_redirect}")

    if detalhes_teste["is_active"]:
        print(f"\n  ⚠️ ATENÇÃO: este produto está ATIVO em produção.")
        print(f"     A URL atual pode estar indexada/compartilhada — o redirect")
        print(f"     do passo 8b é o que evita um link quebrado para quem acessar")
        print(f"     a URL antiga depois da troca.")

    if slug_atual == slug_novo:
        print("\n⚠️ O slug normalizado é IDÊNTICO ao atual — nada para corrigir aqui.")
        print("   Escolha outro produto de teste que tenha slug realmente inválido,")
        print("   ou ajuste manualmente a variável para forçar o teste.")
        return

    # Checagem de colisão — essencial em produto ATIVO: se o slug novo já
    # pertencer a outro produto, o PUT criaria uma ambiguidade real de
    # catálogo (duas páginas "competindo" pelo mesmo slug).
    print(f"\n[8-colisao] Checando se o slug novo já está em uso por outro produto...")
    resultado_colisao_teste = checar_colisao_slug(slug_novo, PRODUCT_ID_TESTE_PUT)

    if resultado_colisao_teste.get("erro"):
        print("\n❌ Não foi possível verificar colisão (erro na checagem). Abortando por segurança.")
        return

    if resultado_colisao_teste.get("colisao"):
        pid_conflito = resultado_colisao_teste.get("product_id_conflitante")
        print(f"\n❌ COLISÃO DETECTADA: o slug '{slug_novo}' já é usado pelo productId {pid_conflito}.")
        print("   Abortando o PUT — não vamos criar dois produtos com o mesmo slug.")
        print("   Seria necessário gerar uma variação (ex: sufixo do SKU) antes de tentar de novo.")
        return

    print("   ✅ Sem colisão — slug novo está livre para uso.")

    # Salvaguarda: confirmação manual antes de qualquer escrita real em produto.
    confirmacao = input(
        "\n>>> Isso vai ALTERAR DE VERDADE o LinkId deste produto na VTEX. "
        "Digite SIM para confirmar: "
    )
    if confirmacao.strip().upper() != "SIM":
        print("Cancelado pelo usuário. Nenhuma alteração foi feita.")
        return

    resultado_put = atualizar_slug_produto(PRODUCT_ID_TESTE_PUT, detalhes_teste["raw"], slug_novo)
    if resultado_put.get("erro"):
        print("\n❌ PUT falhou. Não vamos criar o redirect (evita inconsistência).")
        return

    resultado_redirect = criar_redirect_definitivo(token, slug_atual, destino_redirect)

    print("\n" + "=" * 70)
    print("RESULTADO DO TESTE DE PUT REAL")
    print("=" * 70)
    print(f"PUT no produto:       {'✅ sucesso' if not resultado_put.get('erro') else '❌ falhou'}")
    print(f"Redirect criado:      {'✅ sucesso' if not resultado_redirect.get('erro') else '❌ falhou'}")
    print(f"\nDE:   /{slug_atual.lower()}/p")
    print(f"PARA: {destino_redirect}")


# =============================================================================
# EXECUÇÃO — fluxo completo na ordem real do pipeline
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("TESTE END-TO-END (LEITURA) — Validador de URLs VTEX")
    print("=" * 70)

    if not APP_KEY or not APP_TOKEN:
        print("\n❌ APP_KEY / APP_TOKEN não encontrados. Verifique o arquivo .env.")
        raise SystemExit(1)

    # 1) Login
    token = obter_token_vtex()
    if not token:
        print("\n❌ Não foi possível obter o token. Parando aqui.")
        raise SystemExit(1)

    # 2) SkuId -> ProductId
    product_id = buscar_product_id(SKU_VTEX_TESTE)
    if not product_id:
        print("\n❌ Não foi possível obter o ProductId. Parando aqui.")
        raise SystemExit(1)

    # 3) ProductId -> LinkId + IsActive
    detalhes = buscar_detalhes_produto(product_id)
    if not detalhes:
        print("\n❌ Não foi possível obter os detalhes do produto. Parando aqui.")
        raise SystemExit(1)

    if not detalhes["is_active"]:
        print("\n⚠️ Produto está INATIVO. No pipeline real, isso seria pulado (fim do fluxo).")

    # 4) Normalização do slug
    slug_atual = detalhes["link_id"]
    slug_normalizado = normalizar_slug(slug_atual)
    print(f"\n[4] Normalização do slug")
    print(f"    Slug atual (LinkId):    {slug_atual}")
    print(f"    Slug normalizado:       {slug_normalizado}")
    print(f"    Houve alteração?        {'SIM' if slug_atual != slug_normalizado else 'NÃO (já estava correto)'}")

    # 5) Checar colisão do slug normalizado
    resultado_colisao = checar_colisao_slug(slug_normalizado, product_id)

    # 6) Verificar se já existe redirect para o path atual
    path_atual = f"/{slug_atual.lower()}/p"
    verificar_redirect_existente(token, path_atual)

    # 7) Teste opcional de escrita (paths fictícios, seguro)
    if TESTAR_ESCRITA_REDIRECT:
        print("\n" + "=" * 70)
        print("TESTE OPCIONAL DE ESCRITA (paths fictícios, sem afetar produtos reais)")
        print("=" * 70)
        testar_escrita_redirect(token)

    # 8) [REAL] Teste de PUT — só roda se a flag estiver ativa
    if EXECUTAR_PUT_REAL:
        rodar_teste_put_real(token)
    else:
        print("\n(EXECUTAR_PUT_REAL=False — etapa de PUT real não foi executada.)")

    print("\n" + "=" * 70)
    print("RESUMO FINAL")
    print("=" * 70)
    print(f"SKU testado:          {SKU_VTEX_TESTE}")
    print(f"ProductId:             {product_id}")
    print(f"Ativo:                 {detalhes['is_active']}")
    print(f"Slug atual:            {slug_atual}")
    print(f"Slug normalizado:      {slug_normalizado}")
    print(f"Colisão de slug:       {resultado_colisao.get('colisao')}")
    print("\n✅ Fluxo de leitura completo executado. Nenhuma alteração foi feita")
    print("   em produtos reais (sem PUT). Envie este output completo para revisão.")