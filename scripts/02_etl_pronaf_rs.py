# -*- coding: utf-8 -*-
"""
02_etl_pronaf_rs.py — TRATAMENTO + MODELO DIMENSIONAL + CARGA (RF-01)  — v3

Estrutura esperada (mesma do 01_download_sicor.js):
    <raiz>/scripts/02_etl_pronaf_rs.py     (este arquivo)
    <raiz>/data/raw/sicor/              (saída do 01_download_sicor.js: .gz + tabelas de domínio .csv)
    <raiz>/data/raw/atlas_digital/      (export CSV do Atlas Digital de Desastres)
    <raiz>/data/raw/ibge/               (JSON da API de localidades do IBGE)
    <raiz>/data/staging/                (gerado)
    <raiz>/data/dw/                     (gerado — esquema estrela em CSV)
    <raiz>/data/dw_pronaf.db            (gerado — SQLite para o Power BI)
    <raiz>/data/ml/ml_cluster_municipio.csv  (lido, se existir — gerado UMA vez pelo 03_ml_clusters.py)

Janela: safras 2019/20 a 2024/25 (01/07/2019 a 30/06/2025).

v3: a única mudança em relação à v2 é que a dim_municipio passa a reaproveitar o cluster já
calculado pelo 03_ml_clusters.py (data/ml/ml_cluster_municipio.csv). Assim a carga agendada
reconstrói o DW sem apagar o resultado do modelo, e o ML não precisa ser reexecutado.
Todas as demais regras, filtros e agregações são idênticas.

Execução:
    pip install pandas openpyxl requests
    python 02_etl_pronaf_rs.py                 # full reload (script dentro da árvore do projeto)
    python 02_etl_pronaf_rs.py --so-headers    # só imprime os cabeçalhos dos arquivos brutos e sai
    python 02_etl_pronaf_rs.py --raiz "D:\\Cursos\\POS ABI\\Projetos Integrados de Analytics"
                                            # força a raiz (a pasta que contém data\raw)

Na PRIMEIRA execução rode com --so-headers e confira se as colunas de MAPA_OP
e do Atlas batem; ajuste os candidatos se o Bacen tiver renomeado algo.
"""
from pathlib import Path
import csv
import gzip
import json
import re
import sqlite3
import sys
import unicodedata

import pandas as pd

# ----------------------------------------------------------------------------
# CONFIGURAÇÃO
# ----------------------------------------------------------------------------
def descobrir_raiz():
    """Raiz do projeto = pasta que contém data/raw.
    Ordem: --raiz <caminho>  >  variável de ambiente PRONAF_RAIZ  >  busca subindo a partir do script."""
    if "--raiz" in sys.argv:
        return Path(sys.argv[sys.argv.index("--raiz") + 1]).expanduser().resolve()
    import os
    if os.environ.get("PRONAF_RAIZ"):
        return Path(os.environ["PRONAF_RAIZ"]).resolve()
    aqui = Path(__file__).resolve().parent
    for cand in (aqui, *aqui.parents):
        if (cand / "data" / "raw").is_dir():
            return cand
    sys.exit(f"[ERRO] não encontrei uma pasta 'data/raw' acima de {aqui}.\n"
             "       Rode com:  python 02_etl_pronaf_rs.py --raiz \"D:\\Cursos\\POS ABI\\Projetos Integrados de Analytics\"")


ROOT = descobrir_raiz()
RAW = ROOT / "data" / "raw"
RAW_SICOR, RAW_ATLAS, RAW_IBGE = RAW / "sicor", RAW / "atlas_digital", RAW / "ibge"
STG, DW = ROOT / "data" / "staging", ROOT / "data" / "dw"
DB = ROOT / "data" / "dw_pronaf.db"
LOG = ROOT / "data" / "etl_log.txt"
ML_CLUSTER = ROOT / "data" / "ml" / "ml_cluster_municipio.csv"   # saída do 03_ml_clusters.py

UF = "RS"
DATA_INI, DATA_FIM = pd.Timestamp("2019-07-01"), pd.Timestamp("2025-06-30")
SAFRA_INI, SAFRA_FIM = "2019/2020", "2024/2025"
CHUNK = 500_000

# Formato dos CSVs do DW (data/dw): padrão pt-BR, lido nativamente pelo Power BI/Excel em português
CSV_DW = dict(sep=";", decimal=",", encoding="utf-8-sig", index=False, date_format="%Y-%m-%d",
              float_format="%.4f")

DOMINIOS = {  # chave lógica -> nome do arquivo em data/raw/sicor
    "programa": "Programa.csv",
    "subprograma": "SubProgramas.csv",
    "fonte_recurso": "FonteRecursos.csv",
    "categoria_emitente": "CategoriaEmitente.csv",
    "instrumento_credito": "InstrumentoCredito.csv",
    "empreendimento": "Empreendimento.csv",
    "ifs": "SICOR_LISTA_IFS.csv",
}

# nome lógico -> candidatos de coluna (já normalizados) no arquivo de operações
MAPA_OP = {
    "ref_bacen": ("ref_bacen",),
    "nu_ordem": ("nu_ordem",),
    "uf": ("cd_estado", "sg_uf", "uf"),
    "dt_emissao": ("dt_emissao",),
    "cd_programa": ("cd_programa",),
    "cd_subprograma": ("cd_subprograma",),
    "cd_fonte_recurso": ("cd_fonte_recurso",),
    "cnpj_if": ("cnpj_if",),
    "cd_categoria_emitente": ("cd_categ_emitente", "cd_categoria_emitente"),
    "cd_instrumento": ("cd_inst_credito", "cd_instrumento_credito"),
    "cd_empreendimento": ("cd_empreendimento",),
    "valor": ("vl_parc_credito",),
    "taxa_juro": ("vl_juros",),
    "area": ("vl_area_financ",),
}
ESSENCIAIS = ("ref_bacen", "nu_ordem", "uf", "dt_emissao", "cd_programa", "valor")

# classificação da tipologia COBRADE em classes de interesse
CLASSE_EVENTO = [  # ordem importa: primeira chave que casar vence
    ("Seca", ("estiagem", "seca")),
    ("Excesso hídrico", ("inunda", "enxurrada", "alagamento", "chuvas intensas", "chuva intensa")),
    ("Tempestade", ("granizo", "vendaval", "tornado", "ciclone", "tempestade")),
]
CLASSES_FLAG = ("Seca", "Excesso hídrico")   # classes que marcam a safra como afetada

_log_lines = []


def log(msg=""):
    print(msg)
    _log_lines.append(str(msg))


# ----------------------------------------------------------------------------
# utilidades
# ----------------------------------------------------------------------------
def norm(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", s.lower().strip().lstrip("#")).strip("_")


def _bad_lines_kw(acao):
    """compatibilidade: pandas >= 1.3 usa on_bad_lines; anteriores usam error_bad_lines"""
    if tuple(int(x) for x in pd.__version__.split(".")[:2]) >= (1, 3):
        return {"on_bad_lines": acao}
    return {"error_bad_lines": False, "warn_bad_lines": True} if acao == "skip" else {}


def detectar_sep(caminho, enc):
    with open(caminho, encoding=enc, errors="replace") as f:
        linha = f.readline()
    return max((";", "\t", "|", ","), key=linha.count)


def ler_csv(caminho, **kw):
    """leitura tolerante das tabelas de domínio do Bacen:
    separador detectado pela 1ª linha (sem csv.Sniffer), cascata de encodings e
    cascata de tratamento de aspas: normal -> aspas literais -> descarte de linhas inválidas"""
    caminho = Path(caminho)
    ultimo = None
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            sep = detectar_sep(caminho, enc)
        except UnicodeDecodeError:
            continue
        tentativas = (
            ("normal", {}),
            ("aspas literais", {"quoting": csv.QUOTE_NONE}),
            ("aspas literais + descarte de linhas inválidas",
             {"quoting": csv.QUOTE_NONE, **_bad_lines_kw("skip")}),
        )
        for modo, extra in tentativas:
            try:
                df = pd.read_csv(caminho, sep=sep, encoding=enc, dtype=str, **extra, **kw)
            except UnicodeDecodeError as e:
                ultimo = e
                break                      # tenta próximo encoding
            except (pd.errors.ParserError, ValueError) as e:
                ultimo = e
                continue                   # tenta próximo modo
            if modo != "normal":
                log(f"[AVISO] {caminho.name}: lido em modo '{modo}' — confira se perdeu linhas")
            df.columns = [norm(c.strip().strip('"')) for c in df.columns]
            for c in df.columns:
                df[c] = df[c].astype(str).str.strip().str.strip('"').replace({"nan": None})
            return df
    raise RuntimeError(f"não foi possível ler {caminho.name}: {ultimo}")


def num(serie, rotulo=None):
    """converte texto em número detectando a convenção decimal pela amostra:
    '1.234,56' (pt-BR) | '1234.56' (ponto decimal) | '1234,56' (vírgula decimal)"""
    s = serie.fillna("").astype(str).str.strip()
    amostra = s[s != ""].head(5000)
    tem_virg, tem_pto = amostra.str.contains(",").mean(), amostra.str.contains(r"\.").mean()
    if tem_virg > 0:                       # vírgula presente -> vírgula é o decimal
        conv = "pt-BR (1.234,56)" if tem_pto > 0 else "vírgula decimal (1234,56)"
        s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    else:                                  # só ponto (ou nada) -> ponto é o decimal
        conv = "ponto decimal (1234.56)"
    if rotulo:
        log(f"     {rotulo}: convenção numérica {conv}; amostra {list(amostra.head(3))}")
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


FORMATOS_DATA = ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                 "%d-%m-%Y", "%Y%m%d", "%m/%d/%Y", "%d/%m/%y")


def data(serie, rotulo=None):
    """parse de datas sem depender de format='mixed' (pandas < 2.0): testa formatos
    explícitos numa amostra e aplica o que mais acerta"""
    s = serie.fillna("").astype(str).str.strip()
    amostra = s[s != ""].head(2000)
    melhor, taxa = None, 0.0
    for fmt in FORMATOS_DATA:
        t = pd.to_datetime(amostra, format=fmt, errors="coerce").notna().mean() if len(amostra) else 0
        if t > taxa:
            melhor, taxa = fmt, t
    if melhor and taxa >= 0.9:
        out = pd.to_datetime(s, format=melhor, errors="coerce")
    else:
        melhor, out = "inferido (dayfirst)", pd.to_datetime(s, dayfirst=True, errors="coerce")
    if rotulo:
        log(f"     {rotulo}: formato de data {melhor} ({taxa:.0%} da amostra); "
            f"amostra {list(amostra.head(3))}; NaT: {out.isna().mean():.1%}")
    return out


def col(cols, *cands):
    for c in cands:
        if c in cols:
            return c
    return None


def chave(serie):
    """normaliza chave natural dos dois lados (dimensão e fato): remove espaços, pontuação
    de CNPJ e zeros à esquerda (0001 == 1); vazio -> 'ND'"""
    s = serie.fillna("ND").astype(str).str.strip()
    s = s.where(~s.str.fullmatch(r"[\d.\-/]+"), s.str.replace(r"[.\-/]", "", regex=True))
    s = s.where(~s.str.fullmatch(r"\d+"), s.str.lstrip("0").replace("", "0"))
    return s.replace("", "ND")


def safra(dt):
    """ano-safra jul–jun como 'AAAA/AAAA+1'"""
    a = dt.dt.year
    return (a.where(dt.dt.month >= 7, a - 1)).astype("Int64").astype(str) + "/" + \
        (a.where(dt.dt.month >= 7, a - 1) + 1).astype("Int64").astype(str)


def salvar_dw(df, nome):
    """grava tabela do DW em CSV pt-BR (';' e vírgula decimal)"""
    df.to_csv(DW / f"{nome}.csv", **CSV_DW)


def sk(df, nome):
    df = df.reset_index(drop=True)
    df.insert(0, nome, df.index + 1)
    return df


def com_nd(df, cod, desc="descricao"):
    """adiciona a linha 'Não informado' (chave natural 'ND')"""
    nd = pd.DataFrame({cod: ["ND"], desc: ["Não informado"]})
    return pd.concat([df, nd], ignore_index=True)


def cabecalho_gz(caminho):
    with gzip.open(caminho, "rt", encoding="latin-1", errors="replace") as f:
        return [norm(c) for c in f.readline().rstrip("\n").split(";")]


# ----------------------------------------------------------------------------
# 0. Inspeção de cabeçalhos (--so-headers)
# ----------------------------------------------------------------------------
def inspecionar_headers():
    log("== CABEÇALHOS DOS ARQUIVOS BRUTOS ==")
    for arq in sorted(a for a in RAW_SICOR.glob("*.gz")
                      if "operacao_basica_estado" in a.name.lower())[:1] + \
            [a for a in RAW_SICOR.glob("*.gz") if "complemento" in a.name.lower()]:
        log(f"\n{arq.name}:\n  {cabecalho_gz(arq)}")
    for chave, nome in DOMINIOS.items():
        p = RAW_SICOR / nome
        log(f"\n{nome}: " + (str(list(ler_csv(p).columns)) if p.exists() else "AUSENTE"))
    for arq in arquivos_atlas():
        df = pd.read_excel(arq, nrows=5, dtype=str) if arq.suffix == ".xlsx" else ler_csv(arq, nrows=5)
        log(f"\n{arq.name}:\n  {[norm(c) for c in df.columns]}")
    for arq in arquivos_ibge():
        d = json.loads(arq.read_text(encoding="utf-8"))
        log(f"\n{arq.name}: {len(d)} registros | chaves: {list(d[0].keys()) if d else []}")


def arquivos_atlas():
    return sorted(RAW_ATLAS.glob("*.csv")) + sorted(RAW_ATLAS.glob("*.xlsx"))


def arquivos_ibge():
    return sorted(RAW_IBGE.glob("*.json"))


# ----------------------------------------------------------------------------
# 1. STAGING — domínios
# ----------------------------------------------------------------------------
def stg_dominios():
    log("== staging: tabelas de domínio ==")
    dom = {}
    for chave, nome in DOMINIOS.items():
        p = RAW_SICOR / nome
        if not p.exists():
            log(f"[AVISO] {nome} ausente em {RAW_SICOR} — dimensão só com 'Não informado'")
            dom[chave] = pd.DataFrame()
            continue
        df = ler_csv(p)
        df.to_csv(STG / f"dominio_{chave}.csv", index=False)
        dom[chave] = df
        log(f"[ok] {nome}: {len(df)} linhas | {list(df.columns)}")
    return dom


def codigos_pronaf(dp):
    if dp.empty:
        log("[AVISO] Programa.csv ausente — usando código 0001 (Pronaf)")
        return {"0001", "1"}
    ccod = col(dp.columns, "cd_programa", "codigo")
    cdes = col(dp.columns, "descricao", "programa", "nm_programa") or dp.columns[1]
    d = dp[[ccod, cdes]].dropna()
    up = d[cdes].str.upper()
    cods = set(chave(d.loc[up.str.contains("PRONAF") & ~up.str.contains("PRONAMP"), ccod]))
    log(f"[ok] códigos Pronaf: {sorted(cods)}")
    return cods


# ----------------------------------------------------------------------------
# 2. STAGING — complemento (município por operação)
# ----------------------------------------------------------------------------
def stg_complemento(chaves_op):
    """lê o complemento nacional em blocos e guarda só as operações RS/Pronaf já carregadas
    (qualquer UF de município), casando por chave normalizada"""
    log("== staging: complemento (município IBGE) ==")
    achados = [a for a in RAW_SICOR.glob("*.gz") if a.name.lower() == "sicor_complemento_operacao_basica.gz"]
    p = achados[0] if achados else RAW_SICOR / "SICOR_COMPLEMENTO_OPERACAO_BASICA.gz"
    if not p.exists():
        sys.exit("[ERRO] complemento ausente — rode 01_download_sicor.js")
    partes = []
    with gzip.open(p, "rt", encoding="latin-1", errors="replace") as f:
        for ch in pd.read_csv(f, sep=";", dtype=str, chunksize=CHUNK,
                              quoting=csv.QUOTE_NONE, **_bad_lines_kw("skip")):
            ch.columns = [norm(c) for c in ch.columns]
            cmun = col(ch.columns, "cd_ibge_municipio", "cd_municipio", "codigo_ibge")
            cref, cord = col(ch.columns, "ref_bacen"), col(ch.columns, "nu_ordem")
            if not all([cmun, cref, cord]):
                sys.exit(f"[ERRO] colunas do complemento não identificadas: {list(ch.columns)}")
            part = ch[[cref, cord, cmun]].rename(columns={cref: "ref_bacen", cord: "nu_ordem",
                                                          cmun: "cd_municipio"})
            part["ref_bacen"], part["nu_ordem"] = chave(part["ref_bacen"]), chave(part["nu_ordem"])
            part = part[(part["ref_bacen"] + "|" + part["nu_ordem"]).isin(chaves_op)]
            part["cd_municipio"] = (part["cd_municipio"].fillna("").astype(str).str.strip()
                                    .str.replace(r"\D", "", regex=True).str.zfill(7))
            partes.append(part)
    comp = pd.concat(partes, ignore_index=True).drop_duplicates(subset=["ref_bacen", "nu_ordem"])
    fora = ~comp["cd_municipio"].str.startswith("43")
    log(f"[ok] complemento casado: {len(comp):,} de {len(chaves_op):,} operações "
        f"({len(comp) / len(chaves_op):.1%}); município fora do RS: {fora.sum():,}")
    comp.to_csv(STG / "complemento_rs.csv", index=False)
    return comp


# ----------------------------------------------------------------------------
# 3. STAGING — operações (filtro RS + Pronaf + janela)
# ----------------------------------------------------------------------------
def stg_operacoes(cods):
    log("== staging: operações anuais ==")
    partes, header_logado = [], False
    for arq in sorted(RAW_SICOR.glob("SICOR_OPERACAO_BASICA_ESTADO_*.gz")):
        ano = int(re.search(r"(\d{4})", arq.name).group(1))
        if ano < DATA_INI.year or ano > DATA_FIM.year:
            continue
        log(f"[..] {arq.name}")
        with gzip.open(arq, "rt", encoding="latin-1", errors="replace") as f:
            for ch in pd.read_csv(f, sep=";", dtype=str, chunksize=CHUNK,
                                  quoting=csv.QUOTE_NONE, **_bad_lines_kw("skip")):
                ch.columns = [norm(c) for c in ch.columns]
                if not header_logado:
                    log(f"     colunas: {list(ch.columns)}")
                    header_logado = True
                cols = {k: col(ch.columns, *v) for k, v in MAPA_OP.items()}
                falt = [k for k in ESSENCIAIS if cols[k] is None]
                if falt:
                    sys.exit(f"[ERRO] colunas essenciais ausentes {falt}. Ajuste MAPA_OP. "
                             f"Colunas: {list(ch.columns)}")
                sel = pd.DataFrame({k: (ch[v] if v else "ND") for k, v in cols.items()})
                sel = sel[sel["uf"].str.strip().str.upper() == UF]
                sel = sel[chave(sel["cd_programa"]).isin(cods)]
                if len(sel):
                    partes.append(sel)
    if not partes:
        sys.exit("[ERRO] nenhuma operação RS/Pronaf encontrada — verifique filtros")
    op = pd.concat(partes, ignore_index=True)

    log(f"[ok] operações RS/Pronaf lidas (todas as datas): {len(op):,}")
    op["valor"] = num(op["valor"], "vl_parc_credito")
    op["taxa_juro"], op["area"] = num(op["taxa_juro"]), num(op["area"])
    op["dt_emissao"] = data(op["dt_emissao"], "dt_emissao")
    n0 = len(op)
    sem_data = op["dt_emissao"].isna()
    fora = ~sem_data & ((op["dt_emissao"] < DATA_INI) | (op["dt_emissao"] > DATA_FIM))
    sem_valor = ~sem_data & ~fora & (op["valor"] <= 0)
    log(f"[ok] descartes — sem data: {sem_data.sum():,} | fora da janela: {fora.sum():,} | "
        f"valor<=0: {sem_valor.sum():,}")
    op = op[~sem_data & ~fora & ~sem_valor]
    if op.empty:
        sys.exit("[ERRO] nenhuma operação restou após os filtros — veja amostras acima")
    op["ref_bacen"], op["nu_ordem"] = chave(op["ref_bacen"]), chave(op["nu_ordem"])
    op["ano_mes"] = op["dt_emissao"].dt.year * 100 + op["dt_emissao"].dt.month
    op["ano_safra"] = safra(op["dt_emissao"])
    for c in ("cd_subprograma", "cd_fonte_recurso", "cnpj_if", "cd_categoria_emitente",
              "cd_instrumento", "cd_empreendimento", "cd_programa"):
        op[c] = chave(op[c])
    op.to_csv(STG / "operacoes_pronaf_rs.csv", index=False)
    log(f"[ok] operações RS/Pronaf {SAFRA_INI}–{SAFRA_FIM}: {len(op):,} | "
        f"R$ {op['valor'].sum():,.2f}")
    return op


# ----------------------------------------------------------------------------
# 4. DIMENSÕES
# ----------------------------------------------------------------------------
def dim_generica(dom, cod_cands, desc_cands, nome, nome_cod, filtro=None):
    if dom.empty:
        d = pd.DataFrame({nome_cod: [], "descricao": []})
    else:
        if filtro is not None:
            dom = dom[filtro(dom)]
        ccod = col(dom.columns, *cod_cands) or dom.columns[0]
        cdes = col(dom.columns, *desc_cands) or dom.columns[1]
        d = dom[[ccod, cdes]].rename(columns={ccod: nome_cod, cdes: "descricao"})
        d[nome_cod] = chave(d[nome_cod])
        d["descricao"] = d["descricao"].astype(str).str.strip()
        d = d.drop_duplicates(subset=[nome_cod])
    d = sk(com_nd(d, nome_cod), f"sk_{nome}")
    salvar_dw(d, f"dim_{nome}")
    log(f"[ok] dim_{nome}: {len(d)}")
    return d


def dim_tempo():
    d = pd.DataFrame({"data": pd.date_range(DATA_INI, DATA_FIM, freq="MS")})
    d["ano"], d["mes"] = d.data.dt.year, d.data.dt.month
    d["nome_mes"] = d.mes.map({1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
                               7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez"})
    d["trimestre"] = d.data.dt.quarter
    d["semestre"] = (d.mes > 6).astype(int) + 1
    d["ano_safra"] = safra(d.data)
    d["mes_safra"] = ((d.mes - 7) % 12) + 1          # jul=1 ... jun=12 (compara safras)
    d["periodo_safra"] = d.mes.map({7: "Abertura do Plano Safra (jul-ago)", 8: "Abertura do Plano Safra (jul-ago)",
                                     9: "Plantio de verão (set-nov)", 10: "Plantio de verão (set-nov)",
                                     11: "Plantio de verão (set-nov)",
                                     12: "Condução/colheita (dez-mar)", 1: "Condução/colheita (dez-mar)",
                                     2: "Condução/colheita (dez-mar)", 3: "Condução/colheita (dez-mar)",
                                     4: "Inverno/encerramento (abr-jun)", 5: "Inverno/encerramento (abr-jun)",
                                     6: "Inverno/encerramento (abr-jun)"})
    d["ano_mes"] = d.ano * 100 + d.mes
    d = sk(d, "sk_tempo")
    salvar_dw(d, f"dim_tempo")
    log(f"[ok] dim_tempo: {len(d)} meses")
    return d


def dim_municipio():
    arqs = arquivos_ibge()
    p = arqs[0] if arqs else RAW_IBGE / "ibge_municipios_rs.json"
    if not p.exists():
        RAW_IBGE.mkdir(parents=True, exist_ok=True)
        try:
            import requests
            url = ("https://servicodados.ibge.gov.br/api/v1/localidades/estados/RS/"
                   "municipios?view=nivelado")
            p.write_text(requests.get(url, timeout=60).text, encoding="utf-8")
            log(f"[ok] IBGE baixado em {p}")
        except Exception as e:
            log(f"[AVISO] IBGE indisponível ({e}) — dim_municipio mínima")
    if p.exists():
        d = pd.DataFrame(json.loads(p.read_text(encoding="utf-8")))
        d.columns = [norm(c) for c in d.columns]
        d = d[[col(d.columns, "municipio_id"), col(d.columns, "municipio_nome"),
               col(d.columns, "mesorregiao_nome"), col(d.columns, "regiao_intermediaria_nome"),
               col(d.columns, "regiao_imediata_nome")]]
        d.columns = ["cd_municipio", "nome_municipio", "mesorregiao", "regiao_intermediaria",
                     "regiao_imediata"]
        d["cd_municipio"] = d["cd_municipio"].astype(str).str.zfill(7)
    else:
        d = pd.DataFrame(columns=["cd_municipio", "nome_municipio", "mesorregiao",
                                  "regiao_intermediaria", "regiao_imediata"])
    d["uf"] = UF
    d["cluster_kmeans"] = "Não calculado"   # preenchido na etapa de ML
    d = pd.concat([d, pd.DataFrame({"cd_municipio": ["ND", "FORA_RS"],
                                    "nome_municipio": ["Não informado", "Município fora do RS"],
                                    "uf": [UF, "Outra"]})], ignore_index=True)
    if ML_CLUSTER.exists():   # cluster do modelo treinado uma única vez (03_ml_clusters.py)
        ml = pd.read_csv(ML_CLUSTER, sep=";", encoding="utf-8-sig", dtype=str)
        mapa = dict(zip(ml["cd_municipio"].str.strip(), ml["cluster_kmeans"]))
        d["cluster_kmeans"] = d["cd_municipio"].map(mapa).fillna("Não se aplica")
        log(f"[ok] cluster_kmeans reaproveitado de {ML_CLUSTER.name}: "
            f"{d['cd_municipio'].isin(mapa).sum()} municípios")
    d = sk(d, "sk_municipio")
    salvar_dw(d, f"dim_municipio")
    log(f"[ok] dim_municipio: {len(d)}")
    return d


def dims_empreendimento(de):
    eixos = {"finalidade": ("finalidade",), "atividade": ("atividade",),
             "modalidade": ("modalidade",), "produto": ("produto",)}
    if de.empty:
        ponte = pd.DataFrame(columns=["cd_empreendimento", *eixos])
    else:
        ccod = col(de.columns, "cd_empreendimento", "codigo") or de.columns[0]
        ponte = pd.DataFrame({"cd_empreendimento": chave(de[ccod])})
        for nome, cands in eixos.items():
            c = col(de.columns, *cands)
            ponte[nome] = (de[c].fillna("Não informado").astype(str).str.strip()
                           if c else "Não informado")
    ponte.to_csv(STG / "ponte_empreendimento.csv", index=False)
    dims = {}
    for nome in eixos:
        d = ponte[[nome]].drop_duplicates().rename(columns={nome: "descricao"}) \
            if len(ponte) else pd.DataFrame({"descricao": []})
        d = pd.concat([d, pd.DataFrame({"descricao": ["Não informado"]})]).drop_duplicates()
        d = sk(d, f"sk_{nome}")
        salvar_dw(d, f"dim_{nome}")
        dims[nome] = d
        log(f"[ok] dim_{nome}: {len(d)}")
    return dims, ponte


# ----------------------------------------------------------------------------
# 5. EVENTOS CLIMÁTICOS — Atlas Digital (base S2iD tratada)
# ----------------------------------------------------------------------------
def classificar(tipologia):
    t = norm(tipologia)
    for classe, chaves in CLASSE_EVENTO:
        if any(norm(k) in t for k in chaves):
            return classe
    return "Outro"


def eventos_climaticos():
    arqs = arquivos_atlas()
    if not arqs:
        log(f"[AVISO] export do Atlas não encontrado em {RAW_ATLAS} — flag=0")
        return pd.DataFrame(columns=["cd_municipio", "data", "ano_mes", "ano_safra",
                                     "cobrade", "tipologia", "classe_evento", "status"])
    # aceita 1 ou vários arquivos (ex.: um por ano) — concatena todos
    partes = [pd.read_excel(a, dtype=str) if a.suffix == ".xlsx" else ler_csv(a) for a in arqs]
    df = pd.concat(partes, ignore_index=True)
    df.columns = [norm(c) for c in df.columns]
    log(f"[ok] Atlas: {len(arqs)} arquivo(s), {len(df):,} registros brutos")
    cmun = col(df.columns, "cod_ibge_mun", "cod_ibge", "cd_municipio", "codigo_ibge", "ibge")
    cdt = col(df.columns, "data_evento", "data_ocorrencia", "data_registro", "data")
    ctip = col(df.columns, "descricao_tipologia", "tipologia", "grupo_de_desastre", "tipo")
    ccob = col(df.columns, "cod_cobrade", "cobrade")
    cgru = col(df.columns, "grupo_de_desastre")
    cst = col(df.columns, "status", "status_reconhecimento")
    if not all([cmun, cdt, ctip]):
        sys.exit(f"[ERRO] colunas do Atlas não identificadas: {list(df.columns)}")
    ev = pd.DataFrame({
        "cd_municipio": df[cmun].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        .str.replace(r"\D", "", regex=True).str.zfill(7),
        "data": data(df[cdt], f"Atlas.{cdt}"),
        "cobrade": df[ccob].astype(str).str.strip() if ccob else "ND",
        "tipologia": df[ctip].fillna("Não classificado").astype(str).str.strip(),
        "status": df[cst].fillna("ND").astype(str).str.strip() if cst else "ND",
        "grupo_desastre": df[cgru].fillna("ND").astype(str).str.strip() if cgru else "ND",
        "protocolo": df[col(df.columns, "protocolo_s2id")].astype(str) if col(df.columns, "protocolo_s2id") else "",
    })
    for medida, cands in (("afetados_seca", ("dh_afetados_seca_estiagem",)),
                          ("danos_humanos", ("dh_total_danos_humanos_diretos",)),
                          ("prejuizo_agricultura", ("pepr_agricultura_r",)),
                          ("prejuizo_pecuaria", ("pepr_pecuaria_r",)),
                          ("prejuizo_privado_total", ("pepr_total_privado",))):
        c = col(df.columns, *cands)
        ev[medida] = num(df[c], f"Atlas.{c}" if medida == "prejuizo_agricultura" else None) if c else 0.0
    log(f"     Atlas: amostra cod_ibge {list(ev['cd_municipio'].head(3))} | "
        f"RS (43xxxxx): {ev['cd_municipio'].str.startswith('43').sum():,} | "
        f"sem data: {ev['data'].isna().sum():,}")
    ev = ev[ev["cd_municipio"].str.startswith("43")].dropna(subset=["data"])
    ev = ev[(ev["data"] >= DATA_INI) & (ev["data"] <= DATA_FIM)]
    if cst:  # mantém só reconhecidos, se a coluna existir
        log(f"[ok] Atlas: valores de status: {ev['status'].value_counts().to_dict()}")
        rec = ev["status"].str.upper().str.contains("RECONHEC") & \
            ~ev["status"].str.upper().str.contains("NAO|NÃO")
        log(f"[ok] Atlas: {len(ev):,} registros RS na janela, {rec.sum():,} reconhecidos")
        ev = ev[rec]
    ev["ano_mes"] = ev.data.dt.year * 100 + ev.data.dt.month
    ev["ano_safra"] = safra(ev["data"])
    ev["classe_evento"] = ev["tipologia"].map(classificar)
    ev.to_csv(STG / "eventos_rs.csv", index=False)
    log(f"[ok] eventos por classe: {ev['classe_evento'].value_counts().to_dict()}")
    return ev


# ----------------------------------------------------------------------------
# 6. FATOS
# ----------------------------------------------------------------------------
def construir_fatos(op, comp, dims, ponte, d_tempo, d_mun, ev):
    log("== fatos ==")
    op = op.merge(comp, on=["ref_bacen", "nu_ordem"], how="left")
    sem = op["cd_municipio"].isna()
    fora = ~sem & ~op["cd_municipio"].str.startswith("43", na=False)
    log(f"[qualidade] operações sem registro no complemento: {sem.mean():.1%} | "
        f"município fora do RS: {fora.mean():.1%}")
    if sem.any():
        log(f"     amostra de chaves sem complemento: "
            f"{op.loc[sem, ['ref_bacen', 'nu_ordem', 'dt_emissao']].head(3).values.tolist()}")
    op["cd_municipio"] = op["cd_municipio"].fillna("ND").mask(fora, "FORA_RS")

    op = op.merge(ponte, on="cd_empreendimento", how="left")
    for c in ("finalidade", "atividade", "modalidade", "produto"):
        op[c] = op[c].fillna("Não informado")

    # flag de safra afetada: (município, safra) com evento Seca / Excesso hídrico
    cls = ev[ev["classe_evento"].isin(CLASSES_FLAG)]
    mapa = (cls.groupby(["cd_municipio", "ano_safra"])["classe_evento"]
            .agg(lambda s: "Ambos" if s.nunique() > 1 else s.iloc[0]).reset_index()
            .rename(columns={"classe_evento": "tipo_evento_safra"}))
    op = op.merge(mapa, on=["cd_municipio", "ano_safra"], how="left")
    op["tipo_evento_safra"] = op["tipo_evento_safra"].fillna("Sem evento")
    op["flag_safra_afetada"] = (op["tipo_evento_safra"] != "Sem evento").astype(int)

    grao = ["ano_mes", "cd_municipio", "cd_programa", "cd_subprograma", "finalidade", "atividade",
            "modalidade", "produto", "cd_fonte_recurso", "cnpj_if", "cd_categoria_emitente",
            "cd_instrumento", "flag_safra_afetada", "tipo_evento_safra"]
    op["vl_tp"] = op["valor"] * op["taxa_juro"]
    fato = op.groupby(grao, as_index=False).agg(
        qtde_contratos=("valor", "size"), valor_contratado=("valor", "sum"),
        area_financiada_ha=("area", "sum"), vl_taxa_ponderada=("vl_tp", "sum"))

    fato = fato.merge(d_tempo[["sk_tempo", "ano_mes"]], on="ano_mes", how="left")
    fato = fato.merge(d_mun[["sk_municipio", "cd_municipio"]], on="cd_municipio", how="left")
    for nome, colnat in (("programa", "cd_programa"), ("subprograma", "cd_subprograma"),
                         ("fonte_recurso", "cd_fonte_recurso"),
                         ("instituicao_financeira", "cnpj_if"),
                         ("categoria_emitente", "cd_categoria_emitente"),
                         ("instrumento_credito", "cd_instrumento")):
        dim = dims[nome]
        cod = [c for c in dim.columns if c not in (f"sk_{nome}", "descricao")][0]
        fato = fato.merge(dim[[f"sk_{nome}", cod]].rename(columns={cod: colnat}),
                          on=colnat, how="left")
        nd = int(dim.loc[dim[cod] == "ND", f"sk_{nome}"].iloc[0])
        orf = fato[f"sk_{nome}"].isna().sum()
        if orf:
            log(f"[qualidade] {orf} linhas com código não cadastrado em dim_{nome} → 'ND'")
        fato[f"sk_{nome}"] = fato[f"sk_{nome}"].fillna(nd).astype(int)
    for nome in ("finalidade", "atividade", "modalidade", "produto"):
        fato = fato.merge(dims[nome].rename(columns={"descricao": nome}), on=nome, how="left")
    salvar_dw(fato, f"fato_contratacao_pronaf")
    log(f"[ok] fato_contratacao_pronaf: {len(fato):,} linhas | "
        f"órfãs tempo/município: {fato.sk_tempo.isna().sum()}/{fato.sk_municipio.isna().sum()}")

    # fato de eventos + dim_evento_climatico
    if len(ev):
        dim_ev = sk(ev[["cobrade", "tipologia", "grupo_desastre", "classe_evento"]]
                    .drop_duplicates(subset=["cobrade", "tipologia"]), "sk_evento_climatico")
        fev = ev.merge(dim_ev[["sk_evento_climatico", "cobrade", "tipologia"]],
                       on=["cobrade", "tipologia"], how="left")
        fev = fev.merge(d_tempo[["sk_tempo", "ano_mes"]], on="ano_mes", how="left")
        fev = fev.merge(d_mun[["sk_municipio", "cd_municipio"]], on="cd_municipio", how="left")
        fev["qtde_reconhecimentos"] = 1
        fev = fev[["sk_tempo", "sk_municipio", "sk_evento_climatico", "cd_municipio", "data",
                   "ano_safra", "protocolo", "status", "qtde_reconhecimentos", "afetados_seca",
                   "danos_humanos", "prejuizo_agricultura", "prejuizo_pecuaria",
                   "prejuizo_privado_total"]]
    else:
        dim_ev = sk(pd.DataFrame({"cobrade": ["ND"], "tipologia": ["Não informado"],
                                  "grupo_desastre": ["ND"], "classe_evento": ["Outro"]}),
                    "sk_evento_climatico")
        fev = pd.DataFrame(columns=["sk_tempo", "sk_municipio", "sk_evento_climatico",
                                    "cd_municipio", "data", "ano_safra", "status",
                                    "qtde_reconhecimentos"])
    salvar_dw(dim_ev, f"dim_evento_climatico")
    salvar_dw(fev, f"fato_evento_climatico")
    log(f"[ok] fato_evento_climatico: {len(fev):,} | dim_evento_climatico: {len(dim_ev)}")
    return fato


# ----------------------------------------------------------------------------
# 7. CARGA — SQLite (Power BI em modo Import) + totais para homologação
# ----------------------------------------------------------------------------
def carregar_sqlite():
    log("== carga: SQLite ==")
    con = sqlite3.connect(DB)
    for csv in sorted(DW.glob("*.csv")):
        pd.read_csv(csv, sep=";", decimal=",", encoding="utf-8-sig") \
          .to_sql(csv.stem, con, if_exists="replace", index=False)
        log(f"[ok] {csv.stem}")
    q = """SELECT t.ano_safra, SUM(f.qtde_contratos), ROUND(SUM(f.valor_contratado),2)
           FROM fato_contratacao_pronaf f JOIN dim_tempo t USING(sk_tempo)
           GROUP BY 1 ORDER BY 1"""
    log("\n[homologação] totais por safra — comparar com MDCR / Painel Pronaf (MDA):")
    for s, n, v in con.execute(q):
        log(f"   {s}: {int(n):>9,} contratos | R$ {v:>18,.2f}")
    con.close()
    log(f"\nCarga concluída: {DB}")


def main():
    log(f"pandas {pd.__version__} | python {sys.version.split()[0]}")
    log(f"Raiz do projeto : {ROOT}")
    for nome, pasta in (("SICOR", RAW_SICOR), ("Atlas", RAW_ATLAS), ("IBGE", RAW_IBGE)):
        n = len(list(pasta.glob("*"))) if pasta.is_dir() else None
        log(f"  {nome:<6}: {pasta}  -> " + (f"{n} arquivo(s)" if n is not None else "PASTA NÃO EXISTE"))
    if not RAW_SICOR.is_dir() or not any(RAW_SICOR.glob("SICOR_OPERACAO_BASICA_ESTADO_*.gz")):
        sys.exit("[ERRO] nenhum SICOR_OPERACAO_BASICA_ESTADO_<ano>.gz em data/raw/sicor — "
                 "confira a pasta acima ou use --raiz.")
    log()
    for p in (STG, DW):
        p.mkdir(parents=True, exist_ok=True)
    if "--so-headers" in sys.argv:
        inspecionar_headers()
    else:
        dom = stg_dominios()
        cods = codigos_pronaf(dom["programa"])
        op = stg_operacoes(cods)
        comp = stg_complemento(set(op["ref_bacen"] + "|" + op["nu_ordem"]))
        d_tempo, d_mun = dim_tempo(), dim_municipio()
        dims = {
            "programa": dim_generica(dom["programa"], ("cd_programa", "codigo"),
                                     ("descricao", "programa"), "programa", "cd_programa"),
            "subprograma": dim_generica(
                dom["subprograma"], ("codigo_subprograma", "cd_subprograma", "codigo"),
                ("descricao_subprograma", "descricao", "subprograma"), "subprograma",
                "cd_subprograma",
                filtro=lambda d: chave(d[col(d.columns, "codigo_programa", "cd_programa")]).isin(cods)
                if col(d.columns, "codigo_programa", "cd_programa") else pd.Series(True, index=d.index)),
            "fonte_recurso": dim_generica(dom["fonte_recurso"], ("cd_fonte_recurso", "codigo"),
                                          ("descricao", "fonte_recurso"), "fonte_recurso",
                                          "cd_fonte_recurso"),
            "instituicao_financeira": dim_generica(dom["ifs"], ("cnpj_if", "cnpj", "codigo"),
                                                   ("nome", "nome_if", "razao_social",
                                                    "descricao"),
                                                   "instituicao_financeira", "cnpj_if"),
            "categoria_emitente": dim_generica(dom["categoria_emitente"],
                                               ("cd_categ_emitente", "cd_categoria_emitente",
                                                "codigo"),
                                               ("descricao", "categoria"),
                                               "categoria_emitente", "cd_categoria_emitente"),
            "instrumento_credito": dim_generica(dom["instrumento_credito"],
                                                ("cd_inst_credito", "cd_instrumento_credito",
                                                 "codigo"),
                                                ("descricao", "instrumento"),
                                                "instrumento_credito", "cd_instrumento"),
        }
        de, ponte = dims_empreendimento(dom["empreendimento"])
        dims.update(de)
        ev = eventos_climaticos()
        construir_fatos(op, comp, dims, ponte, d_tempo, d_mun, ev)
        carregar_sqlite()
    LOG.write_text("\n".join(_log_lines), encoding="utf-8")
    print(f"\nLog salvo em {LOG}")


if __name__ == "__main__":
    main()