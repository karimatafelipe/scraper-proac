"""
Scraper - Vitrine de Projetos ProAC ICMS → Supabase
Autor: gerado para Felipe
Uso: python3 scraper_proac.py
"""

import os
import re
import time
import json
import requests
from html.parser import HTMLParser
from dotenv import load_dotenv

load_dotenv()

# ─── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

SITE_URL  = "https://vitrinedeprojetos.cultura.sp.gov.br"
BASE_URL  = f"{SITE_URL}/projetos"
PAGE_SIZE = 9
DELAY     = 1.0

NEXT_ACTION = "409f7e88cc74a85f1b7fec4bf1de1ea6fd100abd76"
STATE_TREE  = "%5B%22%22%2C%7B%22children%22%3A%5B%22(public)%22%2C%7B%22children%22%3A%5B%22projetos%22%2C%7B%22children%22%3A%5B%22__PAGE__%22%2C%7B%7D%2C%22%2Fprojetos%3Fpage%3D1%26filterType%3Dcaptando%26sortOption%3DpublicationAsc%22%2C%22refresh%22%5D%7D%5D%7D%5D%7D%2Cnull%2Cnull%2Ctrue%5D"

# ─── HTML stripper ─────────────────────────────────────────────────────────────

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

# ─── RSC parser ────────────────────────────────────────────────────────────────

def extract_json_from_rsc(text: str) -> dict | None:
    """
    Extrai o JSON de projetos da resposta RSC do Next.js.
    O JSON fica embutido na linha longa da resposta no formato:
    {"projects":[...],"totalPages":N}
    """
    matches = re.findall(r'\{"projects":\[.*\],"totalPages":\d+\}', text)
    if matches:
        try:
            return json.loads(matches[0])
        except json.JSONDecodeError:
            pass

    # Fallback: busca por posição
    idx = text.find('{"projects":[')
    if idx >= 0:
        # Tenta parsear a partir dali
        sub = text[idx:]
        # Encontra o fechamento correto do JSON
        depth = 0
        for i, ch in enumerate(sub):
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(sub[:i+1])
                    except json.JSONDecodeError:
                        break
    return None

# ─── Fetch page ────────────────────────────────────────────────────────────────

def fetch_page(page: int, filter_type: str = "captando") -> dict | None:
    body = f'[{{"filterType":"{filter_type}","search":"","page":{page},"pageSize":{PAGE_SIZE},"sortOption":"publicationAsc"}}]'

    try:
        resp = requests.post(
            BASE_URL,
            params={
                "page": page,
                "filterType": filter_type,
                "sortOption": "publicationAsc",
            },
            headers={
                "Accept": "text/x-component",
                "Content-Type": "text/plain;charset=UTF-8",
                "Content-Length": str(len(body.encode("utf-8"))),
                "Next-Action": NEXT_ACTION,
                "Next-Router-State-Tree": STATE_TREE,
                "Origin": SITE_URL,
                "Referer": f"{BASE_URL}?page={page}&filterType={filter_type}&sortOption=publicationAsc",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.2 Safari/605.1.15",
                "Accept-Language": "pt-BR,pt;q=0.9",
            },
            data=body.encode("utf-8"),
            timeout=30,
        )
        resp.encoding = 'utf-8'
        resp.raise_for_status()
        result = extract_json_from_rsc(resp.text)
        if not result:
            print(f"\n  ⚠️  Não encontrou JSON na p{page}. Preview: {resp.text[:150]}")
        return result
    except requests.RequestException as e:
        print(f"  ⚠️  Erro na página {page}: {e}")
        return None

# ─── Parse project ─────────────────────────────────────────────────────────────

def fix_encoding(text: str) -> str:
    if not text:
        return text
    try:
        return text.encode("latin-1").decode("utf-8")
    except:
        return text

def parse_project(p: dict) -> dict:
    pub_date = p.get("publishDateOfficialDiary", "")
    ano = None
    if pub_date:
        try:
            ano = int(pub_date[:4])
        except ValueError:
            pass

    summary = p.get("summary", "")
    if summary.startswith("$"):
        summary = ""

    return {
        "id":              p.get("submissionNumber", ""),
        "nome":            fix_encoding(p.get("projectName", "")),
        "proponente":      p.get("personType", ""),
        "area":            fix_encoding(p.get("segment", "")),
        "cidade":          fix_encoding(p.get("executionCities", "")),
        "ano":             ano,
        "valor":           p.get("approvedProacValue"),
        "captado":         p.get("capturedValue"),
        "status":          "captando",
        "descricao":       fix_encoding(strip_html(summary)),
        "numero_processo": p.get("id", ""),
        "edital":          None,
    }

# ─── Supabase upsert ───────────────────────────────────────────────────────────

def upsert_batch(rows: list, filter_type: str) -> bool:
    # Injeta o filter_type como status
    for r in rows:
        r["status"] = filter_type

    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/projetos",
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

def run(filter_type: str = "captando"):
    print(f"\n🚀 Iniciando scraping — filtro: '{filter_type}'")

    data = fetch_page(1, filter_type)
    if not data:
        print("❌ Falha ao buscar a primeira página. Abortando.")
        return

    total_pages = data.get("totalPages", 1)
    print(f"📄 Total de páginas: {total_pages}")

    all_projects = []

    for page in range(1, total_pages + 1):
        print(f"  📥 Página {page}/{total_pages}...", end=" ", flush=True)

        page_data = data if page == 1 else fetch_page(page, filter_type)

        if page > 1:
            time.sleep(DELAY)

        if not page_data:
            print("⚠️  pulada")
            continue

        rows = [parse_project(p) for p in page_data.get("projects", [])]
        all_projects.extend(rows)

        ok = upsert_batch(rows, filter_type)
        print(f"✅ {len(rows)} projetos" if ok else "❌ falha no upsert")

    print(f"\n🎉 Concluído! Total processado: {len(all_projects)} projetos")


if __name__ == "__main__":
    run(filter_type="captando")
    run(filter_type="todos")
