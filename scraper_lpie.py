"""
Scraper - LPIE (Lei Paulista de Incentivo ao Esporte) → Supabase
Autor: gerado para Felipe
Uso: python3 scraper_lpie.py
"""

import os
import re
import time
import requests
from html.parser import HTMLParser
from dotenv import load_dotenv

load_dotenv()

# ─── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

BASE_URL  = "http://www.lpie.sp.gov.br/ConsultaPublica/Lista"
DELAY     = 0.3

# ─── HTML helpers ──────────────────────────────────────────────────────────────

class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return " ".join(self.fed).strip()

def strip_html(html: str) -> str:
    if not html:
        return ""
    s = MLStripper()
    s.feed(html)
    return s.get_data()

def fix_encoding(text: str) -> str:
    if not text:
        return text
    try:
        return text.encode("latin-1").decode("utf-8")
    except:
        return text

# ─── Fetch lista ───────────────────────────────────────────────────────────────

def fetch_lista(ano_inicio: int = 2024) -> str | None:
    """Busca a página HTML com a lista de projetos."""
    try:
        resp = requests.get(
            BASE_URL,
            params={
                "DataCadastroInicio": f"01/01/{ano_inicio} 00:00:00",
                "DataCadastroFim":    "31/12/2099 00:00:00",
                "CodigoSegmentoCultural": "0",
                "CodigoProjeto": "0",
                "LocalProponente": "0",
                "LocalRealizacao": "0",
            },
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.2 Safari/605.1.15",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "pt-BR,pt;q=0.9",
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.content.decode("utf-8", errors="replace")
    except requests.RequestException as e:
        print(f"❌ Erro ao buscar lista: {e}")
        return None

# ─── Parse tabela ──────────────────────────────────────────────────────────────

def parse_tabela(html: str) -> list[dict]:
    """Extrai os projetos da tabela HTML."""
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
    projetos = []

    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        if len(cells) < 5:
            continue

        # Extrai o IdProjeto do link
        id_match = re.search(r'IdProjeto=(\d+)', row)
        if not id_match:
            continue
        projeto_id = id_match.group(1)

        data_cadastro  = strip_html(cells[0]).strip()
        nome           = fix_encoding(strip_html(cells[1]).strip())
        proponente     = fix_encoding(strip_html(cells[2]).strip())
        segmento       = fix_encoding(strip_html(cells[3]).strip())
        status         = fix_encoding(strip_html(cells[4]).strip())

        projetos.append({
            "id":               projeto_id,
            "nome":             nome,
            "proponente":       proponente,
            "segmento":         segmento,
            "status":           status,
            "data_cadastro":    data_cadastro,
            "local_proponente": None,
            "local_realizacao": None,
            "valor":            None,
            "descricao":        None,
        })

    return projetos

# ─── Fetch detalhes ────────────────────────────────────────────────────────────

def fetch_detalhes(projeto_id: str) -> dict:
    """Busca detalhes adicionais do projeto (valor, local, descrição)."""
    try:
        resp = requests.get(
            "http://www.lpie.sp.gov.br/ConsultaPublicaImprimir/Create",
            params={
                "IdProjeto": projeto_id,
                "IdUsuario": "0",
                "IdConta": "0",
                "CodigoEmp": "0",
            },
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.2 Safari/605.1.15",
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=20,
        )
        html = resp.content.decode("utf-8", errors="replace")

        # Extrai valor aprovado
        valor = None
        valor_match = re.search(r'Valor[^<]*Aprovado[^<]*</[^>]+>\s*<[^>]+>\s*R\$\s*([\d\.,]+)', html, re.IGNORECASE)
        if not valor_match:
            valor_match = re.search(r'R\$\s*([\d\.,]+)', html)
        if valor_match:
            try:
                valor_str = valor_match.group(1).replace(".", "").replace(",", ".")
                valor = float(valor_str)
            except:
                pass

        # Extrai local do proponente
        local_prop = None
        local_match = re.search(r'Munic[íi]pio[^<]*</[^>]+>\s*<[^>]+>\s*([^<]+)', html, re.IGNORECASE)
        if local_match:
            local_prop = fix_encoding(local_match.group(1).strip())

        # Extrai descrição/objetivo
        descricao = None
        desc_match = re.search(r'Objetivo[^<]*</[^>]+>\s*<[^>]+>\s*([^<]+)', html, re.IGNORECASE)
        if desc_match:
            descricao = fix_encoding(desc_match.group(1).strip())

        return {
            "valor":            valor,
            "local_proponente": local_prop,
            "descricao":        descricao,
        }
    except requests.RequestException:
        return {}

# ─── Supabase upsert ───────────────────────────────────────────────────────────

def upsert_batch(rows: list) -> bool:
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/projetos_lpie",
        headers={
            "apikey":        SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type":  "application/json",
            "Prefer":        "resolution=merge-duplicates,return=minimal",
        },
        json=rows,
        timeout=30,
    )
    if resp.status_code in (200, 201):
        return True
    print(f"  ❌ Supabase erro {resp.status_code}: {resp.text[:300]}")
    return False

# ─── Main ──────────────────────────────────────────────────────────────────────

def run(ano_inicio: int = 2024):
    print(f"\n🚀 Iniciando scraping LPIE — a partir de {ano_inicio}")

    html = fetch_lista(ano_inicio)
    if not html:
        print("❌ Falha ao buscar lista. Abortando.")
        return

    projetos = parse_tabela(html)
    print(f"📋 Projetos encontrados: {len(projetos)}")

    for i, p in enumerate(projetos, 1):
        print(f"  📥 {i}/{len(projetos)} — {p['nome'][:50]}...", end=" ", flush=True)

        # Busca detalhes
        detalhes = fetch_detalhes(p["id"])
        p.update(detalhes)
        time.sleep(DELAY)

        # Upsert individual
        ok = upsert_batch([p])
        print(f"✅" if ok else "❌")

    print(f"\n🎉 Concluído! Total processado: {len(projetos)} projetos LPIE")


if __name__ == "__main__":
    run(ano_inicio=2024)
