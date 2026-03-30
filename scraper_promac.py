"""
Scraper - PROMAC (Programa Municipal de Acesso à Cultura - SP) → Supabase
Extrai projetos aprovados 2025 do PDF oficial
Autor: gerado para Felipe
Uso: python3 scraper_promac.py
"""

import os
import re
import requests
import pdfplumber
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# URL do PDF — atualizar quando sair o de 2026
PDF_URL = "https://www.prefeitura.sp.gov.br/cidade/secretarias/cultura/promac/index.php?p=347724"
PDF_LOCAL = "Catalogo_2025_TEXTO_EXTRAIVEL_compressed.pdf"

# Mapeamento de páginas por área (baseado no sumário)
AREAS_PAGINAS = {
    4:   "Artes Plásticas, Visuais e Design",
    23:  "Bibliotecas, Arquivos e Espaços Culturais",
    30:  "Bolsas de Estudo",
    32:  "Cinemas e Séries de Televisão",
    50:  "Circo",
    54:  "Cultura Digital",
    57:  "Cultura Popular e Artesanato",
    63:  "Dança",
    69:  "Design de Moda",
    72:  "Eventos Carnavalescos e Escolas de Samba",
    74:  "Hip Hop",
    79:  "Literatura",
    91:  "Museu",
    96:  "Música",
    125: "Ópera",
    127: "Patrimônio Histórico e Artístico",
    131: "Pesquisa e Documentação",
    133: "Programas de Rádio e Televisão",
    137: "Projetos Especiais",
    156: "Restauração e Conservação",
    158: "Teatro",
    186: "Vídeo e Fotografia",
}

def get_area(pagina_num):
    area = "Geral"
    for p, a in sorted(AREAS_PAGINAS.items()):
        if pagina_num >= p:
            area = a
    return area

def parse_valor(s):
    if not s:
        return None
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return round(float(s), 2)
    except:
        return None

def extrair_projetos_pagina(text, area):
    projetos = []
    partes = re.split(r'(\d{4}\.\d{2}\.\d{2}/\d+)', text)

    for i in range(0, len(partes) - 1, 2):
        bloco_anterior = partes[i] if i < len(partes) else ""
        protocolo = partes[i+1] if i+1 < len(partes) else None
        bloco_posterior = partes[i+2] if i+2 < len(partes) else ""

        if not protocolo:
            continue

        valores = re.findall(r'R\$\s*([\d\.,]+)', bloco_anterior)
        valor = parse_valor(valores[-1]) if valores else None

        bloco_match = re.search(r'BLOCO\s*\n?\s*(\d)', bloco_anterior)
        bloco = bloco_match.group(1) if bloco_match else None

        dist_match = re.search(r'DISTRITO:\s*\n?\s*([^\n;CONTATO]+)', bloco_anterior, re.IGNORECASE)
        distrito = dist_match.group(1).strip() if dist_match else None

        cont_match = re.search(r'CONTATO:\s*\n?\s*([^\n]+)', bloco_anterior, re.IGNORECASE)
        contato = cont_match.group(1).strip() if cont_match else None

        pchave_match = re.search(r'PALAVRAS-CHAVE:\s*\n?\s*([^\n]+(?:;[^\n]*)*)', bloco_anterior, re.IGNORECASE)
        palavras_chave = pchave_match.group(1).strip() if pchave_match else None

        linhas_pos = [l.strip() for l in bloco_posterior.split('\n') if l.strip() and len(l.strip()) > 3]
        proponente = linhas_pos[0] if linhas_pos else None

        linhas_ant = [l.strip() for l in bloco_anterior.split('\n') if l.strip()]
        nome = None
        for linha in linhas_ant:
            if (len(linha) > 5
                and not re.match(r'^(R\$|BLOCO|DISTRITO|CONTATO|PALAVRAS|PROTOCOLO|\d)', linha)
                and not re.search(r'[a-z]{3}[A-Z]{2}', linha)
                ):
                nome = linha
                break

        if protocolo and valor:
            projetos.append({
                "id":            protocolo,
                "nome":          nome,
                "proponente":    proponente,
                "area":          area,
                "valor":         valor,
                "bloco":         bloco,
                "distrito":      distrito,
                "contato":       contato,
                "palavras_chave": palavras_chave,
                "status":        "aprovado",
                "ano":           2025,
            })

    return projetos

def processar_pdf(pdf_path_or_bytes):
    all_projetos = []
    if isinstance(pdf_path_or_bytes, bytes):
        pdf = pdfplumber.open(BytesIO(pdf_path_or_bytes))
    else:
        pdf = pdfplumber.open(pdf_path_or_bytes)

    with pdf:
        total = len(pdf.pages)
        print(f"  📄 {total} páginas")
        for i in range(4, total):
            page = pdf.pages[i]
            text = page.extract_text() or ""
            if not text or not re.search(r'\d{4}\.\d{2}\.\d{2}/\d+', text):
                continue
            area = get_area(i + 1)
            projetos = extrair_projetos_pagina(text, area)
            all_projetos.extend(projetos)

    return all_projetos

def upsert_batch(rows):
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/projetos_promac",
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

def run():
    print("\n🚀 Iniciando scraper PROMAC 2025")

    # Usa PDF local se existir, senão tenta baixar
    if os.path.exists(PDF_LOCAL):
        print(f"📂 Usando PDF local: {PDF_LOCAL}")
        projetos = processar_pdf(PDF_LOCAL)
    else:
        print(f"📥 Baixando PDF...")
        try:
            resp = requests.get(PDF_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
            resp.raise_for_status()
            projetos = processar_pdf(resp.content)
        except Exception as e:
            print(f"❌ Erro ao baixar PDF: {e}")
            return

    print(f"\n📋 Projetos extraídos: {len(projetos)}")

    total_ok = 0
    for i in range(0, len(projetos), 50):
        lote = projetos[i:i+50]
        ok = upsert_batch(lote)
        if ok:
            total_ok += len(lote)
        print(f"  {'✅' if ok else '❌'} Lote {i//50 + 1}: {len(lote)} projetos")

    print(f"\n🎉 Concluído! {total_ok} projetos PROMAC subidos no Supabase")

if __name__ == "__main__":
    run()
