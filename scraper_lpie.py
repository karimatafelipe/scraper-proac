"""
Scraper - LPIE (Lei Paulista de Incentivo ao Esporte) → Supabase
Extrai dados dos PDFs oficiais de projetos aprovados
Autor: gerado para Felipe
Uso: python3 scraper_lpie.py
"""

import os
import re
import time
import requests
import fitz  # pymupdf
from dotenv import load_dotenv

load_dotenv()

# ─── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

BASE_DOC_URL = "http://www.lpie.sp.gov.br/DocumentosCadastro/Buscar"

# PDFs conhecidos por ano — o script tenta descobrir novos automaticamente
PDFS_CONHECIDOS = {
    2024: 353934,
}

# Range de IDs para buscar PDFs novos (varre em torno dos conhecidos)
BUSCA_RANGE = 500

# ─── Descoberta automática de novos PDFs ───────────────────────────────────────

def descobrir_novos_pdfs(ultimo_id_conhecido: int) -> dict[int, int]:
    """
    Tenta encontrar PDFs de anos mais recentes varrendo IDs acima do último conhecido.
    Retorna {ano: id_pdf} para os novos encontrados.
    """
    print(f"🔍 Buscando novos PDFs de projetos aprovados (IDs {ultimo_id_conhecido+1} a {ultimo_id_conhecido+BUSCA_RANGE})...")
    novos = {}

    for doc_id in range(ultimo_id_conhecido + 1, ultimo_id_conhecido + BUSCA_RANGE):
        try:
            resp = requests.get(
                f"{BASE_DOC_URL}/{doc_id}",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            if resp.status_code == 200 and resp.content[:4] == b'%PDF':
                # Verifica se é um PDF da LPIE de projetos aprovados
                doc = fitz.open(stream=resp.content, filetype="pdf")
                texto_p1 = doc[0].get_text()[:500]
                if "LEI PAULISTA DE INCENTIVO AO ESPORTE" in texto_p1 and "Projetos Aprovados" in texto_p1:
                    # Extrai o ano do título
                    ano_match = re.search(r'em (\d{4})', texto_p1)
                    if ano_match:
                        ano = int(ano_match.group(1))
                        novos[ano] = doc_id
                        print(f"  ✅ Novo PDF encontrado! Ano: {ano}, ID: {doc_id}")
                doc.close()
        except Exception:
            continue
        time.sleep(0.2)

    if not novos:
        print("  ℹ️  Nenhum PDF novo encontrado.")
    return novos

# ─── Download e parse do PDF ───────────────────────────────────────────────────

def download_pdf(doc_id: int) -> bytes | None:
    try:
        resp = requests.get(
            f"{BASE_DOC_URL}/{doc_id}",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.content
    except requests.RequestException as e:
        print(f"❌ Erro ao baixar PDF {doc_id}: {e}")
        return None

def parse_valor(texto: str) -> float | None:
    """Converte 'R$ 1.234,56' para float."""
    if not texto:
        return None
    texto = texto.replace("R$", "").strip()
    texto = texto.replace(".", "").replace(",", ".")
    try:
        return float(texto)
    except ValueError:
        return None

def parse_percentual(texto: str) -> float | None:
    """Converte '38,46%' para float."""
    if not texto:
        return None
    texto = texto.replace("%", "").replace(",", ".").strip()
    try:
        return float(texto)
    except ValueError:
        return None

def parse_pdf(pdf_bytes: bytes, ano: int) -> list[dict]:
    """Extrai projetos do PDF da LPIE."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    projetos = []

    # Concatena todo o texto do PDF
    texto_completo = ""
    for page in doc:
        texto_completo += page.get_text() + "\n"
    doc.close()

    # Padrão de cada projeto:
    # CÓDIGO_LPIE\nCÓDIGO_SEFAZ\nNOME PROPONENTE\nNOME PROJETO\nR$ VALOR_APROV\nR$ VALOR_CAPT\nPERCENT% CNPJ\nÁREA\nMUNICÍPIOS\nPÚBLICO\nMODALIDADE
    # Usa o código LPIE (4 dígitos) como âncora
    blocos = re.split(r'\n(?=\d{4}\n\d{6}\n)', texto_completo)

    for bloco in blocos:
        linhas = [l.strip() for l in bloco.strip().split('\n') if l.strip()]
        if len(linhas) < 6:
            continue

        # Verifica se começa com código LPIE (4 dígitos) e SEFAZ (6 dígitos)
        if not re.match(r'^\d{4}$', linhas[0]):
            continue
        if not re.match(r'^\d{6}$', linhas[1]):
            continue

        codigo_lpie  = linhas[0]
        codigo_sefaz = linhas[1]

        # Proponente e nome do projeto ficam antes do valor
        # Procura a linha com R$
        idx_valor = None
        for i, l in enumerate(linhas):
            if re.match(r'^R\$', l):
                idx_valor = i
                break

        if idx_valor is None or idx_valor < 3:
            continue

        proponente = " ".join(linhas[2:idx_valor-1])
        nome_projeto = linhas[idx_valor-1]

        valor_aprovado = parse_valor(linhas[idx_valor]) if idx_valor < len(linhas) else None
        valor_captado  = parse_valor(linhas[idx_valor+1]) if idx_valor+1 < len(linhas) else None

        # Percentual e CNPJ ficam na mesma linha: "38,46% 09.093.751/0001-74"
        perc_cnpj_line = linhas[idx_valor+2] if idx_valor+2 < len(linhas) else ""
        perc_match  = re.match(r'^([\d,\.]+)%\s*([\d\.\/\-]+)', perc_cnpj_line)
        percentual  = parse_percentual(perc_match.group(1)) if perc_match else None
        cnpj        = perc_match.group(2).strip() if perc_match else None

        area        = linhas[idx_valor+3] if idx_valor+3 < len(linhas) else None
        municipios  = linhas[idx_valor+4] if idx_valor+4 < len(linhas) else None

        # Modalidade é a última linha do bloco
        modalidade  = linhas[-1] if linhas else None

        # Público alvo é tudo entre municípios e modalidade
        publico_alvo = " ".join(linhas[idx_valor+5:-1]) if idx_valor+5 < len(linhas)-1 else None

        projetos.append({
            "id":                 codigo_lpie,
            "nome":               nome_projeto,
            "proponente":         proponente,
            "segmento":           area,
            "status":             "aprovado",
            "data_cadastro":      None,
            "local_proponente":   None,
            "local_realizacao":   municipios,
            "valor":              valor_aprovado,
            "descricao":          None,
            "codigo_sefaz":       codigo_sefaz,
            "cnpj":               cnpj,
            "area_atuacao":       area,
            "modalidade":         modalidade,
            "percentual_captado": percentual,
            "valor_captado":      valor_captado,
            "publico_alvo":       publico_alvo,
            "ano":                ano,
        })

    return projetos

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

def run():
    print("\n🚀 Iniciando scraper LPIE — PDFs de projetos aprovados")

    # Descobre novos PDFs automaticamente
    ultimo_id = max(PDFS_CONHECIDOS.values())
    novos = descobrir_novos_pdfs(ultimo_id)
    todos_pdfs = {**PDFS_CONHECIDOS, **novos}

    print(f"\n📋 PDFs a processar: {todos_pdfs}")

    total_geral = 0

    for ano, doc_id in sorted(todos_pdfs.items()):
        print(f"\n📄 Processando PDF {ano} (ID: {doc_id})...")

        pdf_bytes = download_pdf(doc_id)
        if not pdf_bytes:
            continue

        projetos = parse_pdf(pdf_bytes, ano)
        print(f"  📊 Projetos extraídos: {len(projetos)}")

        if not projetos:
            print("  ⚠️  Nenhum projeto extraído — verifique o formato do PDF")
            continue

        # Upsert em lotes de 50
        lote_size = 50
        for i in range(0, len(projetos), lote_size):
            lote = projetos[i:i+lote_size]
            ok = upsert_batch(lote)
            print(f"  {'✅' if ok else '❌'} Lote {i//lote_size + 1}: {len(lote)} projetos")

        total_geral += len(projetos)

    print(f"\n🎉 Concluído! Total processado: {total_geral} projetos LPIE aprovados")


if __name__ == "__main__":
    run()
