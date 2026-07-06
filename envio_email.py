# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Relatório por E-mail: Normalização de URLs
# MAGIC
# MAGIC **Projeto:** Validador de URLs VTEX (Bemol)
# MAGIC
# MAGIC Adaptado do modelo já em produção para Alt Text VTEX (mesmo padrão visual, SMTP e
# MAGIC destinatários), lendo as métricas reais da **última execução** registrada em
# MAGIC `hive_metastore.tabelas_auxiliares.backup_de_validacao_de_url`.
# MAGIC
# MAGIC Pode ser executado de duas formas:
# MAGIC - **Como task separada** após `02_orquestrador_main`, dentro do mesmo Databricks Job
# MAGIC   (lê o `execution_id` via Job Task Values — não precisa adivinhar qual foi a última rodada)
# MAGIC - **Isoladamente**, lendo automaticamente a execução mais recente da tabela de log

# COMMAND ----------

import smtplib
import textwrap
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timezone, timedelta

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÕES SMTP (mesmo padrão do projeto de Alt Text)
# ─────────────────────────────────────────────────────────────
SMTP_SERVER  = "smtp.office365.com"
SMTP_PORT    = 587
SMTP_TIMEOUT = 30
USERNAME     = "fabriciomacedo@bemol.com.br"
PASSWORD     = dbutils.secrets.get(scope="smtp", key="password")  # noqa: F821

# ─────────────────────────────────────────────────────────────
# TABELA DE LOG
# ─────────────────────────────────────────────────────────────
TABELA_LOG    = "hive_metastore.tabelas_auxiliares.backup_de_validacao_de_url"
TABELA_ORIGEM = "bemolonline.backup_conteudo.agente_seo_db"

# ─────────────────────────────────────────────────────────────
# DESTINATÁRIOS (mesmo padrão do projeto de Alt Text)
# ─────────────────────────────────────────────────────────────
TO_RECIPIENTS  = ["fabriciomacedo@bemol.com.br"]
CC_RECIPIENTS  = ["conteudobol@bemol.com.br"]
BCC_RECIPIENTS = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coleta das métricas da execução mais recente

# COMMAND ----------

def _now_manaus() -> str:
    tz_manaus = timezone(timedelta(hours=-4))
    return datetime.now(tz_manaus).strftime("%d/%m/%Y às %H:%M")


def obter_execution_id_via_task_values():
    """Tenta ler o execution_id da task anterior (mesmo Job). Retorna None se não disponível."""
    try:
        return dbutils.jobs.taskValues.get(taskKey="orquestrador", key="execution_id")  # noqa: F821
    except Exception:
        return None


def get_job_stats() -> dict:
    """
    Lê as métricas reais da ÚLTIMA execução diretamente da tabela Delta de log.
    Prioriza o execution_id vindo de Job Task Values (se disponível); caso
    contrário, usa o execution_id mais recente encontrado na própria tabela.
    """
    from pyspark.sql import functions as F

    # 1) Zera o cache agressivo do Delta para essa tabela antes de consultar
    spark.sql(f"REFRESH TABLE {TABELA_LOG}")  # noqa: F821
    df = spark.table(TABELA_LOG)  # noqa: F821

    # Compatibilidade garantida entre schemas velhos (data_execucao) e novos (timestamp)
    # A presença de valores NULL numa dessas colunas fazia o MAX() falhar nas execuções novas.
    col_tempo = F.coalesce(F.col("data_execucao"), F.col("timestamp")) if "data_execucao" in df.columns else F.col("timestamp")

    execution_id = obter_execution_id_via_task_values()

    if not execution_id:
        # Puxa inequivocamente o execution_id usando coalesce para não dar NULL nas novas
        coluna_sql = "coalesce(data_execucao, timestamp)" if "data_execucao" in df.columns else "timestamp"
        query_ultimo = f"""
            SELECT execution_id, MAX({coluna_sql}) as max_t
            FROM {TABELA_LOG}
            GROUP BY execution_id
            ORDER BY max_t DESC
            LIMIT 1
        """
        ultima_linha = spark.sql(query_ultimo).collect()
        if not ultima_linha:
            raise ValueError("Tabela de log está vazia — nenhuma execução para reportar.")
        execution_id = ultima_linha[0]["execution_id"]

    print(f"DEBUG: Puxando métricas para o Execution ID: {execution_id}")

    df_run = df.filter(F.col("execution_id") == execution_id)

    if df_run.count() == 0:
        raise ValueError(f"Nenhuma linha encontrada para execution_id={execution_id}.")

    agg = df_run.agg(
        F.count("*").alias("total"),
        F.count(F.when(F.col("status") == "SUCESSO", True)).alias("sucesso"),
        F.count(F.when(F.col("status") == "PULADO_INATIVO", True)).alias("pulado_inativo"),
        F.count(F.when(F.col("status") == "PULADO_JA_CORRIGIDO", True)).alias("pulado_ja_corrigido"),
        F.count(F.when(F.col("status") == "ERRO_COLISAO_IRRESOLVIVEL", True)).alias("erro_colisao"),
        F.count(F.when(F.col("status") == "ERRO_PUT", True)).alias("erro_put"),
        F.count(F.when(F.col("status") == "ERRO_REDIRECT", True)).alias("erro_redirect"),
        F.count(F.when(F.col("status") == "ERRO_API", True)).alias("erro_api"),
        F.count(F.when(F.col("houve_colisao") == True, True)).alias("total_colisoes"),  # noqa: E712
        F.avg("tempo_processamento_ms").alias("tempo_medio_ms"),
        F.max(col_tempo).alias("ultimo_timestamp"),
        F.min(col_tempo).alias("primeiro_timestamp"),
    ).collect()[0]

    # Lista detalhada dos erros de redirect (caso crítico — PUT ok, redirect falhou)
    skus_erro_redirect = [
        row["sku_vtex"]
        for row in df_run.filter(F.col("status") == "ERRO_REDIRECT").select("sku_vtex").collect()
    ]

    primeiro_ts = agg["primeiro_timestamp"]
    ultimo_ts = agg["ultimo_timestamp"]
    elapsed_sec = int((ultimo_ts - primeiro_ts).total_seconds()) if primeiro_ts and ultimo_ts else 0

    return {
        "execution_id": execution_id,
        "total": agg["total"] or 0,
        "sucesso": agg["sucesso"] or 0,
        "pulado_inativo": agg["pulado_inativo"] or 0,
        "pulado_ja_corrigido": agg["pulado_ja_corrigido"] or 0,
        "erro_colisao": agg["erro_colisao"] or 0,
        "erro_put": agg["erro_put"] or 0,
        "erro_redirect": agg["erro_redirect"] or 0,
        "erro_api": agg["erro_api"] or 0,
        "total_colisoes": agg["total_colisoes"] or 0,
        "tempo_medio_ms": agg["tempo_medio_ms"] or 0,
        "elapsed_sec": elapsed_sec,
        "skus_erro_redirect": skus_erro_redirect,
        "run_ts": ultimo_ts.strftime("%d/%m/%Y %H:%M") if ultimo_ts else "—",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utilitários

# COMMAND ----------

def _fmt_elapsed(seconds: int) -> str:
    h, r = divmod(seconds, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}h {m:02d}m {s:02d}s"

# COMMAND ----------

# MAGIC %md
# MAGIC ## HTML do e-mail
# MAGIC
# MAGIC Mesmo layout visual do relatório de Alt Text (cabeçalho azul Bemol, badge de status,
# MAGIC tabela de métricas), adaptado para as métricas e o status do projeto de URLs.

# COMMAND ----------

BEMOL_LOGO_URL = (
    "https://bemolqa.vtexassets.com/assets/vtex/assets-builder/"
    "bemolqa.store-theme/17.0.3-beta.0/images/"
    "bemol-logo___0e4ce7bac603e6a725fdc3b40ad03e13.svg"
)


def build_email_html(stats: dict) -> str:
    timestamp = _now_manaus()
    elapsed_fmt = _fmt_elapsed(stats.get("elapsed_sec", 0))

    tem_erro_critico = stats["erro_redirect"] > 0
    tem_erro_comum = stats["erro_put"] > 0 or stats["erro_api"] > 0 or stats["erro_colisao"] > 0

    if tem_erro_critico:
        badge_bg = "#c0392b"
        badge_label = "&#128680; ATENÇÃO — POSSÍVEL 404 (redirect falhou)"
    elif tem_erro_comum:
        badge_bg = "#e67e22"
        badge_label = "&#9888;&#65039; CONCLUÍDO COM ERROS"
    else:
        badge_bg = "#27ae60"
        badge_label = "&#9989; EXECUÇÃO SEM ERROS"

    def row(icon, label, value, color="#1a1a1a", bold=False):
        weight = "700" if bold else "400"
        return f"""
        <tr>
          <td style="padding:10px 14px;border-bottom:1px solid #e0e0e0;
                     font-size:14px;color:#1a1a1a;font-family:Arial,sans-serif;">
            {icon}&nbsp; {label}
          </td>
          <td style="padding:10px 14px;border-bottom:1px solid #e0e0e0;
                     text-align:right;font-size:14px;font-weight:{weight};
                     color:{color};font-family:Arial,sans-serif;">
            {value}
          </td>
        </tr>"""

    rows_html = "".join([
        row("&#128202;", "Total de SKUs processados", stats["total"], bold=True),
        row("&#9989;", "Corrigidos com sucesso", stats["sucesso"], "#27ae60", bold=True),
        row("&#9654;&#65039;", "Pulados — já corretos", stats["pulado_ja_corrigido"], "#1a1a1a"),
        row("&#9208;&#65039;", "Pulados — produto inativo", stats["pulado_inativo"], "#1a1a1a"),
        row("&#128279;", "Colisões de slug detectadas", stats["total_colisoes"], "#1a1a1a"),
        row("&#10060;", "Erros de colisão irresolúvel", stats["erro_colisao"],
            "#c0392b" if stats["erro_colisao"] else "#1a1a1a"),
        row("&#10060;", "Erros de atualização (PUT)", stats["erro_put"],
            "#c0392b" if stats["erro_put"] else "#1a1a1a"),
        row("&#128680;", "Erros de redirect (⚠️ crítico)", stats["erro_redirect"],
            "#c0392b" if stats["erro_redirect"] else "#1a1a1a", bold=stats["erro_redirect"] > 0),
        row("&#10060;", "Erros de API (leitura)", stats["erro_api"],
            "#c0392b" if stats["erro_api"] else "#1a1a1a"),
    ])

    bloco_alerta_redirect = ""
    if stats["skus_erro_redirect"]:
        skus_lista = ", ".join(stats["skus_erro_redirect"][:10])
        sufixo = "..." if len(stats["skus_erro_redirect"]) > 10 else ""
        bloco_alerta_redirect = f"""
          <tr>
            <td style="padding:0 28px 24px;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
                     style="background-color:#fdecea;border:1px solid #c0392b;
                            border-radius:6px;border-collapse:collapse;">
                <tr>
                  <td style="padding:12px 14px;font-size:12px;color:#7a1f1f;
                             font-family:Arial,sans-serif;">
                    <strong>&#128680; AÇÃO MANUAL NECESSÁRIA:</strong> os SKUs abaixo tiveram o
                    slug atualizado (PUT confirmado), mas o redirect da URL antiga falhou.
                    Isso significa que a URL antiga pode estar retornando 404 agora.
                    Crie o redirect manualmente o quanto antes para estes SKUs:
                    <br><br>
                    <span style="font-family:'Courier New',monospace;">{skus_lista}{sufixo}</span>
                  </td>
                </tr>
              </table>
            </td>
          </tr>"""

    html = f"""\
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Relatório — Automação Bemol: Normalização de URLs</title>
</head>
<body style="margin:0;padding:0;background-color:#f4f4f4;
             font-family:Arial,Helvetica,sans-serif;-webkit-text-size-adjust:100%;">

  <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
         style="background-color:#f4f4f4;padding:30px 0;">
    <tr>
      <td align="center">

        <table role="presentation" width="580" cellpadding="0" cellspacing="0"
               style="max-width:580px;width:100%;background-color:#ffffff;
                      border-radius:8px;overflow:hidden;
                      box-shadow:0 2px 8px rgba(0,0,0,0.10);">

          <!-- Cabeçalho azul Bemol -->
          <tr>
            <td style="background-color:#004f9f;padding:20px 28px;">
              <!--[if !mso]><!-->
              <img src="{BEMOL_LOGO_URL}" alt="Bemol" width="110" height="auto"
                   style="display:block;height:auto;max-height:40px;
                          border:0;outline:none;text-decoration:none;">
              <!--<![endif]-->
              <!--[if mso]>
              <span style="font-family:Arial,sans-serif;font-size:26px;
                           font-weight:900;color:#ffffff;letter-spacing:1px;">
                bemol
              </span>
              <![endif]-->
            </td>
          </tr>

          <!-- Título -->
          <tr>
            <td style="padding:24px 28px 8px;">
              <p style="margin:0;font-size:20px;font-weight:700;color:#1a1a1a;
                        font-family:Arial,sans-serif;">
                Relatório &mdash; Automação&nbsp;Bemol:
                <span style="background-color:#ffd700;color:#1a1a1a;
                             padding:1px 6px;border-radius:3px;">SEO</span>
                Normalização de URLs
              </p>
              <p style="margin:6px 0 0;font-size:12px;color:#444444;
                        font-family:Arial,sans-serif;">
                Gerado em {timestamp} (horário de Manaus)
                &nbsp;|&nbsp; Duração da execução: {elapsed_fmt}
              </p>
            </td>
          </tr>

          <!-- Badge de status -->
          <tr>
            <td style="padding:12px 28px 20px;">
              <span style="display:inline-block;background-color:{badge_bg};
                           color:#ffffff;font-size:13px;font-weight:700;
                           padding:7px 16px;border-radius:20px;
                           font-family:Arial,sans-serif;">
                {badge_label}
              </span>
            </td>
          </tr>

          <!-- Tabela de métricas -->
          <tr>
            <td style="padding:0 28px 24px;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
                     style="border:1px solid #e0e0e0;border-radius:6px;
                            overflow:hidden;border-collapse:collapse;">
                <thead>
                  <tr style="background-color:#2c2c2c;">
                    <th style="padding:10px 14px;text-align:left;font-size:13px;
                               color:#ffffff;font-family:Arial,sans-serif;font-weight:700;">
                      Métrica
                    </th>
                    <th style="padding:10px 14px;text-align:right;font-size:13px;
                               color:#ffffff;font-family:Arial,sans-serif;font-weight:700;">
                      Quantidade
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {rows_html}
                </tbody>
              </table>
            </td>
          </tr>

          {bloco_alerta_redirect}

          <!-- Detalhes de execução -->
          <tr>
            <td style="padding:0 28px 24px;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
                     style="background-color:#f8f8f8;border:1px solid #e0e0e0;
                            border-radius:6px;border-collapse:collapse;">
                <tr>
                  <td style="padding:10px 14px;font-size:12px;color:#1a1a1a;
                             font-family:Arial,sans-serif;border-bottom:1px solid #e8e8e8;">
                    <strong>Execution ID:</strong>&nbsp;
                    <span style="font-family:'Courier New',monospace;font-size:11px;">
                      {stats.get("execution_id", "—")}
                    </span>
                  </td>
                </tr>
                <tr>
                  <td style="padding:10px 14px;font-size:12px;color:#1a1a1a;
                             font-family:Arial,sans-serif;border-bottom:1px solid #e8e8e8;">
                    <strong>Tabela de log:</strong>&nbsp;
                    <span style="font-family:'Courier New',monospace;font-size:11px;">
                      {TABELA_LOG}
                    </span>
                  </td>
                </tr>
                <tr>
                  <td style="padding:10px 14px;font-size:12px;color:#1a1a1a;
                             font-family:Arial,sans-serif;">
                    <strong>Fonte:</strong>&nbsp;
                    <span style="font-family:'Courier New',monospace;font-size:11px;">
                      {TABELA_ORIGEM}
                    </span>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Rodapé -->
          <tr>
            <td style="background-color:#f0f0f0;padding:14px 28px;
                       border-top:1px solid #e0e0e0;">
              <p style="margin:0;font-size:11px;color:#444444;
                        font-family:Arial,sans-serif;">
                Mensagem automática &mdash; Automação&nbsp;Bemol
                <span style="background-color:#ffd700;color:#1a1a1a;
                             padding:1px 4px;border-radius:2px;font-size:10px;
                             font-weight:700;">SEO</span>
                &nbsp;Databricks &nbsp;|&nbsp; Processamento de {stats.get('total', 0)} URLs
              </p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>

</body>
</html>"""

    return textwrap.dedent(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Envio do e-mail

# COMMAND ----------

def send_report_email() -> None:
    stats = get_job_stats()

    tem_erro_critico = stats["erro_redirect"] > 0
    tem_erro_comum = stats["erro_put"] > 0 or stats["erro_api"] > 0 or stats["erro_colisao"] > 0

    if tem_erro_critico:
        subject_prefix = "🚨 ATENÇÃO"
    elif tem_erro_comum:
        subject_prefix = "⚠️ COM ERROS"
    else:
        subject_prefix = "✅ SUCESSO"

    subject = (
        f"[Automação Bemol SEO] {subject_prefix} — Normalização de URLs | "
        f"{stats['sucesso']}/{stats['total']} SKUs | {_now_manaus()}"
    )

    plain_text = (
        f"Relatório — Automação Bemol: Normalização de URLs\n"
        f"{'=' * 50}\n"
        f"Gerado em   : {_now_manaus()} (Manaus)\n"
        f"Duração     : {_fmt_elapsed(stats.get('elapsed_sec', 0))}\n\n"
        f"Total processados        : {stats['total']}\n"
        f"Sucesso                  : {stats['sucesso']}\n"
        f"Pulados (já corretos)    : {stats['pulado_ja_corrigido']}\n"
        f"Pulados (inativo)        : {stats['pulado_inativo']}\n"
        f"Colisões detectadas      : {stats['total_colisoes']}\n"
        f"Erro colisão irresolúvel : {stats['erro_colisao']}\n"
        f"Erro PUT                 : {stats['erro_put']}\n"
        f"Erro redirect (CRÍTICO)  : {stats['erro_redirect']}\n"
        f"Erro API (leitura)       : {stats['erro_api']}\n\n"
        f"Execution ID: {stats.get('execution_id')}\n"
        f"Log: {TABELA_LOG}\n"
    )

    if stats["skus_erro_redirect"]:
        plain_text += f"\nSKUs com redirect pendente (AÇÃO MANUAL): {', '.join(stats['skus_erro_redirect'])}\n"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = USERNAME
    msg["To"] = ", ".join(TO_RECIPIENTS)
    if CC_RECIPIENTS:
        msg["Cc"] = ", ".join(CC_RECIPIENTS)

    msg.attach(MIMEText(plain_text, "plain", "utf-8"))
    msg.attach(MIMEText(build_email_html(stats), "html", "utf-8"))

    all_recipients = TO_RECIPIENTS + CC_RECIPIENTS + BCC_RECIPIENTS

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=SMTP_TIMEOUT) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(USERNAME, PASSWORD)
            server.sendmail(USERNAME, all_recipients, msg.as_string())

        print(f"[EMAIL] ✅ Relatório enviado para: {', '.join(all_recipients)}")
        print(f"[EMAIL] Assunto: {subject}")

    except smtplib.SMTPAuthenticationError as exc:
        print(f"[EMAIL] ❌ Falha de autenticação SMTP: {exc}")
        raise
    except smtplib.SMTPException as exc:
        print(f"[EMAIL] ❌ Erro SMTP: {exc}")
        raise
    except Exception as exc:
        print(f"[EMAIL] ❌ Erro inesperado ao enviar e-mail: {exc}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ponto de entrada

# COMMAND ----------

send_report_email()