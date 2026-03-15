"""
Scraper - LIE (Lei Federal de Incentivo ao Esporte) → Supabase
Baixa a planilha XLSX do gov.br e sobe projetos de SP no Supabase
Autor: gerado para Felipe
Uso: python3 scraper_lie.py
"""

import os
import re
import requests
import openpyxl
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ─── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

XLSX_URL = "https://www.gov.br/esporte/pt-br/acoes-e-programas/lei-de-incentivo-ao-esporte/projetos-aptos-a-captacao-atualizada-31-12-25.xlsx/@@download/file"

# Filtra apenas SP ou todos os estados
APENAS_SP = True

# ─── Download planilha ─────────────────────────────────────────────────────────

def download_xlsx() -> bytes | None:
    print("📥 Baixando planilha LIE...")
    try:
        resp = requests.get(
            XLSX_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=60,
        )
        resp.raise_for_status()
        print(f"  ✅ {len(resp.content)//1024} KB baixados")
        return resp.content
    except requests.RequestException as e:
        print(f"  ❌ Erro: {e}")
        return None

# ─── Parse planilha ────────────────────────────────────────────────────────────

def parse_xlsx(xlsx_bytes: bytes) -> list[dict]:
    """Extrai projetos da planilha LIE."""
    wb = openpyxl.load_workbook(BytesIO(xlsx_bytes))
    ws = wb.active

    print(f"  📊 Planilha: {ws.max_row} linhas, {ws.max_column} colunas")

    # Cabeçalhos na linha 2
    # Colunas conhecidas:
    # 1=Nº, 2=Processo, 3=Proponente, 4=Projeto, 5=SLI,
    # 7=Manifestação, 8=Modalidade, 9=CNPJ, 10=Cidade, 11=UF,
    # 12=Valor, 13=Data Publicação, 14=Prazo Captação

    projetos = []
    hoje = datetime.now()

    for row in ws.iter_rows(min_row=3, values_only=True):
        if not row or not row[0]:
            continue

        numero     = str(row[0]).strip() if row[0] else None
        processo   = str(row[1]).strip() if row[1] else None
        proponente = str(row[2]).strip() if row[2] else None
        nome       = str(row[3]).strip() if row[3] else None
        manifest   = str(row[6]).strip() if row[6] else None
        modalidade = str(row[7]).strip() if row[7] else None
        cnpj       = str(row[8]).strip().replace('\xa0', '') if row[8] else None
        cidade     = str(row[9]).strip() if row[9] else None
        uf         = str(row[10]).strip() if row[10] else None
        valor      = row[11]
        data_pub   = str(row[12]).strip() if row[12] else None
        prazo      = str(row[13]).strip() if row[13] else None

        # Filtra por SP se configurado
        if APENAS_SP and uf != "SP":
            continue

        # Converte valor
        if valor:
            try:
                valor = float(str(valor).replace("R$", "").replace(".", "").replace(",", ".").strip())
            except:
                valor = None

        # Determina status baseado no prazo de captação
        status = "encerrado"
        if prazo:
            try:
                # Tenta parsear a data de prazo
                for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"]:
                    try:
                        prazo_dt = datetime.strptime(prazo[:10], fmt)
                        if prazo_dt >= hoje:
                            status = "em_captacao"
                        break
                    except:
                        continue
            except:
                pass

        # ID único: número do processo
        projeto_id = processo if processo else numero

        if not projeto_id:
            continue

        projetos.append({
            "id":             projeto_id,
            "processo":       processo,
            "proponente":     proponente,
            "nome":           nome,
            "manifestacao":   manifest,
            "modalidade":     modalidade,
            "cnpj":           cnpj,
            "uf":             uf,
            "cidade":         cidade,
            "valor":          valor,
            "data_publicacao": data_pub,
            "prazo_captacao": prazo,
            "status":         status,
        })

    return projetos

# ─── Supabase upsert ───────────────────────────────────────────────────────────

def upsert_batch(rows: list) -> bool:
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/projetos_lie",
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

def run():
    print("\n🚀 Iniciando scraper LIE Federal")

    xlsx_bytes = download_xlsx()
    if not xlsx_bytes:
        print("❌ Falha ao baixar planilha. Abortando.")
        return

    projetos = parse_xlsx(xlsx_bytes)
    filtro = "SP" if APENAS_SP else "todos os estados"
    print(f"📋 Projetos encontrados ({filtro}): {len(projetos)}")

    em_captacao = sum(1 for p in projetos if p["status"] == "em_captacao")
    print(f"  🟢 Em captação: {em_captacao}")
    print(f"  ⚫ Encerrados: {len(projetos) - em_captacao}")

    # Upsert em lotes de 100
    total_ok = 0
    for i in range(0, len(projetos), 100):
        lote = projetos[i:i+100]
        ok = upsert_batch(lote)
        if ok:
            total_ok += len(lote)
            print(f"  ✅ Lote {i//100 + 1}: {len(lote)} projetos")
        else:
            print(f"  ❌ Lote {i//100 + 1}: falha")

    print(f"\n🎉 Concluído! {total_ok} projetos LIE salvos no Supabase")


if __name__ == "__main__":
    run()
