"""
=============================================================================
SCRIPT DE TESTE — Validador de URLs VTEX (Bemol)
=============================================================================
Objetivo: validar, de forma isolada, ANTES de gerar os notebooks finais:

  1. Login programático na VTEX (AppKey/AppToken -> VtexIdclientAutCookie)
  2. Consulta GraphQL ao Rewriter para checar se um slug já existe
     (query `internal`)

Não grava nada, não altera nada. Apenas leitura.

COMO USAR:
  1. Preencha as 4 variáveis na seção "CONFIGURAÇÃO" abaixo.
  2. Rode: python teste_verificar_slug.py
  3. Me envie o output completo (prints de cada etapa).

IMPORTANTE DE SEGURANÇA:
  - Não cole esse arquivo com as chaves preenchidas em nenhum lugar público
    (chat, repositório sem .gitignore, etc.).
  - Depois de validar, vamos mover essas credenciais para Databricks Secrets
    — elas NÃO devem ficar hardcoded no notebook final.
=============================================================================
"""

import os
import requests
import json

from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# CONFIGURAÇÃO — preencha aqui antes de rodar
# =============================================================================

ACCOUNT      = "bemol"
BASE_URL     = "https://bemol.vtexcommercestable.com.br"
MYVTEX_URL   = "https://bemol.myvtex.com"

APP_KEY      = os.getenv("APP_KEY", "")
APP_TOKEN    = os.getenv("APP_TOKEN", "")

# Slug que você quer checar se já existe (use o LinkId real, em lowercase,
# já normalizado — pode ser o da camisa que testamos antes)
SLUG_PARA_TESTAR = "camisa-do-brasil-masculina-gg-nike-2026-27-torcedor-pro-amarelo--mp-"

# =============================================================================
# ETAPA 1 — LOGIN: troca AppKey/AppToken por VtexIdclientAutCookie
# =============================================================================

def obter_token_vtex() -> str | None:
    """
    Faz login programático na VTEX usando AppKey/AppToken.
    Retorna o valor do cookie VtexIdclientAutCookie, ou None se falhar.
    """
    url = f"{BASE_URL}/api/vtexid/apptoken/login"
    params = {"an": ACCOUNT}
    headers = {"Accept": "application/json"}
    body = {"appkey": APP_KEY, "apptoken": APP_TOKEN}

    print(f"[1/3] POST {url}?an={ACCOUNT}")

    try:
        resp = requests.post(url, params=params, headers=headers, json=body, timeout=15)
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Erro de conexão: {e}")
        return None

    print(f"   Status HTTP: {resp.status_code}")

    if resp.status_code != 200:
        print(f"   ❌ Corpo da resposta: {resp.text[:500]}")
        return None

    try:
        data = resp.json()
    except ValueError:
        print(f"   ❌ Resposta não é JSON válido: {resp.text[:500]}")
        return None

    print(f"   Corpo da resposta (chaves): {list(data.keys())}")

    # A VTEX pode devolver o token em formatos levemente diferentes
    # dependendo da versão da API — checamos os 2 formatos mais comuns.
    token = data.get("token")
    if not token:
        auth_cookie = data.get("authCookie", {})
        if isinstance(auth_cookie, dict):
            token = auth_cookie.get("Value")

    if token:
        print(f"   ✅ Token obtido (primeiros 20 caracteres): {token[:20]}...")
        return token
    else:
        print(f"   ❌ Token não encontrado na resposta. JSON completo: {json.dumps(data, indent=2)[:1000]}")
        return None


# =============================================================================
# ETAPA 2a — INTROSPECÇÃO: descobre o schema real em vez de adivinhar campos
# =============================================================================

def introspeccionar_schema(token: str) -> dict:
    """
    Faz uma query de introspecção GraphQL para descobrir:
      - Se o campo 'internal' existe no tipo Query, e quais argumentos aceita
      - Quais campos o tipo de retorno (provavelmente 'Route' ou similar) tem

    Isso elimina a necessidade de adivinhar nomes de campo.
    """
    url = f"{MYVTEX_URL}/_v/private/graphql/v1"
    headers = {
        "cookie": f"VtexIdclientAutCookie={token};VtexWorkspace=master%3A-;",
        "Content-Type": "application/json",
    }

    introspection_query = '''
    query IntrospectQueryType {
      __schema {
        queryType {
          fields {
            name
            args {
              name
              type {
                name
                kind
                ofType {
                  name
                  kind
                }
              }
            }
            type {
              name
              kind
              ofType {
                name
                kind
              }
            }
          }
        }
      }
    }
    '''

    print(f"\n[2a] Introspecção do schema GraphQL em {url}")

    try:
        resp = requests.post(url, headers=headers, json={"query": introspection_query}, timeout=30)
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Erro de conexão: {e}")
        return {"erro": True, "msg": str(e)}

    print(f"   Status HTTP: {resp.status_code}")

    try:
        data = resp.json()
    except ValueError:
        print(f"   ❌ Resposta não é JSON: {resp.text[:500]}")
        return {"erro": True, "msg": "Resposta não-JSON", "raw": resp.text[:500]}

    if "errors" in data:
        print(f"   ❌ Introspecção bloqueada/erro: {data['errors']}")
        return {"erro": True, "msg": data["errors"]}

    fields = data.get("data", {}).get("__schema", {}).get("queryType", {}).get("fields", [])

    relevantes = [
        f for f in fields
        if any(termo in f["name"].lower() for termo in ["internal", "redirect", "route", "rewrit"])
    ]

    print(f"   ✅ Introspecção funcionou! Total de campos na Query raiz: {len(fields)}")
    print(f"\n   Campos relevantes encontrados ({len(relevantes)}):")
    for f in relevantes:
        args_str = ", ".join(
            f"{a['name']}: {a['type'].get('name') or a['type'].get('ofType', {}).get('name')}"
            for a in f.get("args", [])
        )
        tipo_retorno = f["type"].get("name") or f["type"].get("ofType", {}).get("name")
        print(f"      • {f['name']}({args_str}) -> {tipo_retorno}")

    if not relevantes:
        print("   ⚠️ Nenhum campo relevante encontrado. Listando TODOS os campos disponíveis:")
        for f in fields:
            print(f"      • {f['name']}")

    return {"erro": False, "fields": fields, "relevantes": relevantes}


def introspeccionar_tipo(token: str, type_name: str) -> dict:
    """
    Dado o nome de um tipo (ex: 'Route', 'Internal'), descobre todos os
    campos e seus tipos. Usado depois de identificar o tipo de retorno
    do campo 'internal' (ou equivalente) na introspecção da Query raiz.
    """
    url = f"{MYVTEX_URL}/_v/private/graphql/v1"
    headers = {
        "cookie": f"VtexIdclientAutCookie={token};VtexWorkspace=master%3A-;",
        "Content-Type": "application/json",
    }

    query = '''
    query IntrospectType($typeName: String!) {
      __type(name: $typeName) {
        name
        kind
        fields {
          name
          type {
            name
            kind
            ofType {
              name
              kind
            }
          }
        }
      }
    }
    '''

    print(f"\n[2b] Introspecção do tipo '{type_name}'")

    try:
        resp = requests.post(
            url, headers=headers,
            json={"query": query, "variables": {"typeName": type_name}},
            timeout=30,
        )
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Erro de conexão: {e}")
        return {"erro": True, "msg": str(e)}

    try:
        data = resp.json()
    except ValueError:
        print(f"   ❌ Resposta não é JSON: {resp.text[:500]}")
        return {"erro": True}

    if "errors" in data:
        print(f"   ❌ Erro: {data['errors']}")
        return {"erro": True, "msg": data["errors"]}

    type_data = data.get("data", {}).get("__type")
    if not type_data:
        print(f"   ⚠️ Tipo '{type_name}' não encontrado.")
        return {"erro": True, "msg": "Tipo não encontrado"}

    print(f"   ✅ Tipo '{type_data['name']}' ({type_data['kind']}) — campos:")
    for f in type_data.get("fields", []) or []:
        tipo = f["type"].get("name") or f["type"].get("ofType", {}).get("name")
        print(f"      • {f['name']}: {tipo}")

    return {"erro": False, "type_data": type_data}


# =============================================================================
# ETAPA 2 — QUERY GRAPHQL: verifica se o slug já existe como rota interna
# =============================================================================

# Cada variação testa uma hipótese diferente sobre onde está o erro de sintaxe.
VARIACOES_QUERY = {
    "A_sem_context_com_variavel": '''
    query CheckSlug($routeId: String!) {
      internal(routeId: $routeId) {
        id
        from
        type
        binding
        resolveAs
      }
    }
    ''',
    "B_sem_context_inline": '''
    query {
      internal(routeId: "__SLUG__") {
        id
        from
        type
        binding
        resolveAs
      }
    }
    ''',
    "C_context_no_campo_query_nao_no_internal": '''
    query CheckSlug($routeId: String!) @context(provider: "vtex.rewriter@1.x") {
      internal(routeId: $routeId) {
        id
        from
        type
        binding
        resolveAs
      }
    }
    ''',
    "D_campos_minimos_so_id_e_from": '''
    query CheckSlug($routeId: String!) {
      internal(routeId: $routeId) {
        id
        from
      }
    }
    ''',
}


def _tentar_variacao(nome: str, query: str, headers: dict, url: str, slug: str) -> dict:
    """Executa uma variação de query e retorna o resultado bruto."""
    query_final = query.replace("__SLUG__", slug)
    variables = {"routeId": slug} if "$routeId" in query else {}
    body = {"query": query_final, "variables": variables}

    print(f"\n   ── Variação [{nome}] ──")
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=30)
    except requests.exceptions.RequestException as e:
        print(f"      ❌ Erro de conexão: {e}")
        return {"erro": True, "msg": str(e)}

    print(f"      Status HTTP: {resp.status_code}")
    try:
        data = resp.json()
    except ValueError:
        print(f"      ❌ Resposta não é JSON: {resp.text[:300]}")
        return {"erro": True, "msg": "Resposta não-JSON", "raw": resp.text[:300]}

    if "errors" in data:
        print(f"      ❌ Erro GraphQL: {data['errors']}")
        return {"erro": True, "msg": data["errors"]}

    internal_data = data.get("data", {}).get("internal")
    print(f"      ✅ SUCESSO! data.internal = {internal_data}")
    return {"erro": False, "existe": internal_data is not None, "raw": internal_data}


def verificar_slug_existe(token: str, slug: str) -> dict:
    """
    Testa múltiplas variações de sintaxe da query GraphQL contra o Rewriter,
    para isolar exatamente qual formato o schema aceita.

    Para de tentar na primeira variação que funcionar (sem erro de validação).
    """
    url = f"{MYVTEX_URL}/_v/private/graphql/v1"
    headers = {
        "cookie": f"VtexIdclientAutCookie={token};VtexWorkspace=master%3A-;",
        "Content-Type": "application/json",
    }

    print(f"\n[2/3] POST {url}")
    print(f"   routeId testado: {slug}")
    print(f"   Testando {len(VARIACOES_QUERY)} variações de sintaxe...")

    for nome, query in VARIACOES_QUERY.items():
        resultado = _tentar_variacao(nome, query, headers, url, slug)
        if not resultado.get("erro"):
            print(f"\n   🎯 Variação '{nome}' FUNCIONOU. Usaremos essa sintaxe no notebook final.")
            return resultado

    print("\n   ⚠️ Nenhuma variação funcionou.")
    return {"erro": True, "msg": "Todas as variações falharam — ver detalhes acima"}


# =============================================================================
# ETAPA 3 — EXECUÇÃO
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("TESTE DE VALIDAÇÃO — Login VTEX + Verificação de Slug (Rewriter)")
    print("=" * 70)

    token = obter_token_vtex()

    if not token:
        print("\n❌ Não foi possível obter o token. Parando aqui.")
        print("   Possíveis causas: AppKey/AppToken incorretos, conta errada,")
        print("   ou a chave não tem permissão de login (apptoken/login).")
        raise SystemExit(1)

    # ── Passo A: tenta introspecção (descobre o schema real) ────────────────
    print("\n" + "=" * 70)
    print("PASSO A — Tentando introspecção do schema (forma confiável)")
    print("=" * 70)

    schema_result = introspeccionar_schema(token)

    if not schema_result.get("erro") and schema_result.get("relevantes"):
        # Descobriu os campos certos — tenta descobrir o tipo de retorno também
        primeiro_campo = schema_result["relevantes"][0]
        tipo_retorno = primeiro_campo["type"].get("name") or primeiro_campo["type"].get("ofType", {}).get("name")
        if tipo_retorno:
            introspeccionar_tipo(token, tipo_retorno)

        print("\n" + "=" * 70)
        print("[3/3] RESULTADO FINAL")
        print("=" * 70)
        print("✅ Introspecção revelou o schema real — me envie todo o output acima.")
        print("   Vou montar a query final com os nomes de campo corretos.")
        raise SystemExit(0)

    print("\n   ⚠️ Introspecção não retornou campos relevantes ou falhou.")
    print("   Isso pode significar que introspecção está desabilitada neste")
    print("   endpoint específico (comum em proxies privados de produção).")

    # ── Passo B: fallback — tenta as variações manuais de query ─────────────
    print("\n" + "=" * 70)
    print("PASSO B — Fallback: testando variações manuais de sintaxe")
    print("=" * 70)

    resultado = verificar_slug_existe(token, SLUG_PARA_TESTAR)

    print("\n" + "=" * 70)
    print("[3/3] RESULTADO FINAL")
    print("=" * 70)

    if resultado.get("erro"):
        print(f"❌ Erro na consulta GraphQL: {resultado.get('msg')}")
        print("   Isso pode indicar: sintaxe da query incorreta, versão do")
        print("   @context errada, ou permissão insuficiente da AppKey no")
        print("   recurso do Rewriter (License Manager).")
        print("\n   PRÓXIMO PASSO RECOMENDADO: use o GraphQL IDE do Admin VTEX")
        print("   (Admin > qualquer página > ?adminAppId=... ou /admin/graphql-ide),")
        print("   selecione o app 'vtex.rewriter' no seletor, e use a aba 'Docs'")
        print("   (canto direito) para ver o schema real documentado pela própria VTEX.")
    else:
        if resultado["existe"]:
            print(f"✅ Slug JÁ EXISTE como rota. Dados: {resultado['raw']}")
        else:
            print(f"✅ Slug LIVRE (não existe rota com esse routeId).")