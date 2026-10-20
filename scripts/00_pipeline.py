# -*- coding: utf-8 -*-
"""
00_pipeline.py — Carga automatizada do projeto Pronaf RS (extração + ETL).

O que roda, em ordem:

    1. sicor_base     01_download_sicor.js   baixa só os arquivos da janela 2019-2025 que ainda
                                              NÃO existem em data/raw/sicor (na prática, só na 1ª carga)
    2. sicor_recente  01_download_sicor.js   rebaixa (--force) os arquivos anuais que cobrem os
                                              ÚLTIMOS 3 MESES, mais o complemento e as tabelas de domínio
    3. atlas          01_download_atlas.mjs  só se data/raw/atlas_digital estiver vazia
    4. ibge           01_download_ibge.js    só se data/raw/ibge estiver vazia
    5. etl            02_etl_pronaf_rs.py    staging, dimensões, fatos, CSVs do DW e SQLite

O que NÃO roda aqui, de propósito:
    - 03_ml_clusters.py: o modelo é treinado UMA vez (80% treino / 20% teste). O ETL apenas
      relê o cluster já calculado em data/ml/ml_cluster_municipio.csv e o regrava na dim_municipio.

Por que "últimos 3 meses" significa "arquivos anuais":
    o Bacen não publica arquivos mensais; ele reescreve o arquivo do ano corrente a cada
    divulgação. Os meses recentes só chegam rebaixando o arquivo anual que os contém.
    Ex.: em set/2026 -> 2026; em jan/2027 -> 2026 e 2027.

Uso:
    python 00_pipeline.py                  # carga agendada (download incremental + ETL)
    python 00_pipeline.py --dry-run        # mostra o plano e os comandos, não executa nada
    python 00_pipeline.py --sem-download   # só o ETL, com os arquivos que já estão em data/raw
    python 00_pipeline.py --carga-completa # rebaixa toda a janela 2019-2025 do SICOR (carga inicial)
    python 00_pipeline.py --meses 6        # muda a janela de atualização (padrão: 3 meses)
    python 00_pipeline.py --raiz "D:\\Cursos\\POS ABI\\Projetos Integrados de Analytics"

Saída: log em data/logs/pipeline_AAAAMMDD_HHMMSS.log
Códigos de saída: 0 = sucesso | 1 = alguma etapa obrigatória falhou | 2 = outra execução em curso
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
import os
import sqlite3
import subprocess
import sys
import time

# ---------------------------------------------------------------------------
# parâmetros do projeto
# ---------------------------------------------------------------------------
ANO_INICIO, ANO_FIM = 2019, 2025    # arquivos anuais que cobrem a janela jul/2019-jun/2025 do ETL
MESES_PADRAO = 3                    # janela de atualização do download incremental
IDADE_MAX_TRAVA = 6 * 3600          # trava mais velha que isso é considerada órfã


def flag(nome: str) -> bool:
    return nome in sys.argv


def valor(nome: str, padrao=None):
    return sys.argv[sys.argv.index(nome) + 1] if nome in sys.argv else padrao


def descobrir_raiz() -> Path:
    if flag("--raiz"):
        return Path(valor("--raiz")).expanduser().resolve()
    if os.environ.get("PRONAF_RAIZ"):
        return Path(os.environ["PRONAF_RAIZ"]).resolve()
    aqui = Path(__file__).resolve().parent
    for cand in (aqui, *aqui.parents):
        if (cand / "data").is_dir():
            return cand
    return aqui.parent


ROOT = descobrir_raiz()
SCRIPTS = Path(__file__).resolve().parent
DATA = ROOT / "data"
RAW_SICOR, RAW_ATLAS, RAW_IBGE = DATA / "raw" / "sicor", DATA / "raw" / "atlas_digital", DATA / "raw" / "ibge"
LOGS = DATA / "logs"
TRAVA = LOGS / "pipeline.lock"
ML_CLUSTER = DATA / "ml" / "ml_cluster_municipio.csv"

DRY_RUN = flag("--dry-run")
SEM_DOWNLOAD = flag("--sem-download")
CARGA_COMPLETA = flag("--carga-completa")
MESES = int(valor("--meses", MESES_PADRAO))

DOMINIOS = ("Programa.csv", "SubProgramas.csv", "FonteRecursos.csv", "CategoriaEmitente.csv",
            "InstrumentoCredito.csv", "Empreendimento.csv", "SICOR_LISTA_IFS.csv")
COMPLEMENTO = "SICOR_COMPLEMENTO_OPERACAO_BASICA.gz"


# ---------------------------------------------------------------------------
# log
# ---------------------------------------------------------------------------
class Log:
    def __init__(self, caminho: Path):
        self.caminho = caminho
        caminho.parent.mkdir(parents=True, exist_ok=True)
        self.f = caminho.open("w", encoding="utf-8")

    def __call__(self, msg: str = ""):
        linha = f"{datetime.now():%Y-%m-%d %H:%M:%S} | {msg}"
        print(linha, flush=True)
        self.f.write(linha + "\n")
        self.f.flush()

    def bruto(self, msg: str):
        print(msg, flush=True)
        self.f.write(msg + "\n")
        self.f.flush()

    def fechar(self):
        self.f.close()


# ---------------------------------------------------------------------------
# regras de "o que baixar"
# ---------------------------------------------------------------------------
def anos_dos_ultimos_meses(n: int, hoje: date | None = None) -> list:
    """Anos dos arquivos anuais que contêm os últimos n meses (mês corrente incluído)."""
    hoje = hoje or date.today()
    a, m, anos = hoje.year, hoje.month, set()
    for _ in range(max(n, 1)):
        anos.add(a)
        m -= 1
        if m == 0:
            a, m = a - 1, 12
    return sorted(anos)


def arquivos_base_faltando() -> list:
    """Arquivos indispensáveis ao ETL que ainda não estão em data/raw/sicor."""
    esperados = [f"SICOR_OPERACAO_BASICA_ESTADO_{a}.gz" for a in range(ANO_INICIO, ANO_FIM + 1)]
    esperados += [COMPLEMENTO, *DOMINIOS]
    return [n for n in esperados if not (RAW_SICOR / n).exists()]


def pasta_vazia(pasta: Path, padroes: tuple) -> bool:
    return not pasta.is_dir() or not any(p for pad in padroes for p in pasta.glob(pad))


# ---------------------------------------------------------------------------
# etapas
# ---------------------------------------------------------------------------
@dataclass
class Etapa:
    nome: str
    descricao: str
    comando: list
    rodar: bool = True          # False = etapa dispensada nesta execução (motivo em 'motivo')
    motivo: str = ""
    tentativas: int = 1
    espera: int = 30            # segundos entre tentativas (dobra a cada falha)
    timeout: int = 3600
    opcional: bool = False      # falha é registrada, mas não interrompe o pipeline
    exige: list = field(default_factory=list)


def montar_etapas() -> list:
    py, node = sys.executable, "node"
    anos = anos_dos_ultimos_meses(MESES)
    faltando = arquivos_base_faltando()

    if CARGA_COMPLETA:
        base_rodar, base_cmd_extra, base_motivo = True, ["--force"], "--carga-completa"
    else:
        base_rodar, base_cmd_extra = bool(faltando), []
        base_motivo = "base histórica completa em data/raw/sicor" if not faltando else ""

    etapas = [
        Etapa("sicor_base",
              f"SICOR {ANO_INICIO}-{ANO_FIM}: baixa só os arquivos ausentes"
              + (f" ({len(faltando)} faltando)" if faltando and not CARGA_COMPLETA else ""),
              [node, str(SCRIPTS / "01_download_sicor.js"),
               "--anos", f"{ANO_INICIO}-{ANO_FIM}", *base_cmd_extra],
              rodar=base_rodar, motivo=base_motivo, tentativas=3, timeout=7200),

        Etapa("sicor_recente",
              f"SICOR últimos {MESES} meses: rebaixa {', '.join(map(str, anos))} "
              "+ complemento + domínios",
              [node, str(SCRIPTS / "01_download_sicor.js"),
               "--anos", f"{anos[0]}-{anos[-1]}", "--force"],
              tentativas=2, timeout=7200, opcional=True),

        Etapa("atlas", "Atlas Digital de Desastres (RS) — só se a pasta estiver vazia",
              [node, str(SCRIPTS / "01_download_atlas.mjs"),
               "--uf", "RS", "--de", str(ANO_INICIO), "--ate", str(ANO_FIM)],
              rodar=pasta_vazia(RAW_ATLAS, ("*.csv", "*.xlsx")),
              motivo="arquivo do Atlas já presente", tentativas=3, timeout=1800),

        Etapa("ibge", "Municípios do RS (API IBGE) — só se a pasta estiver vazia",
              [node, str(SCRIPTS / "01_download_ibge.js")],
              rodar=pasta_vazia(RAW_IBGE, ("*.json",)),
              motivo="arquivo do IBGE já presente", tentativas=3, timeout=600),

        Etapa("etl", "ETL: staging, dimensões, fatos, CSVs do DW e SQLite",
              [py, str(SCRIPTS / "02_etl_pronaf_rs.py"), "--raiz", str(ROOT)],
              timeout=4 * 3600,
              exige=[RAW_SICOR / f"SICOR_OPERACAO_BASICA_ESTADO_{a}.gz"
                     for a in range(ANO_INICIO, ANO_FIM + 1)] + [RAW_SICOR / COMPLEMENTO]),
    ]
    if SEM_DOWNLOAD:
        for e in etapas[:4]:
            e.rodar, e.motivo = False, "--sem-download"
    return etapas


# ---------------------------------------------------------------------------
# trava de execução (evita duas cargas simultâneas disparadas pelo agendador)
# ---------------------------------------------------------------------------
def adquirir_trava(log: Log) -> bool:
    TRAVA.parent.mkdir(parents=True, exist_ok=True)
    if TRAVA.exists():
        idade = time.time() - TRAVA.stat().st_mtime
        if idade < IDADE_MAX_TRAVA:
            conteudo = TRAVA.read_text(encoding="utf-8", errors="replace").strip()
            log(f"Outra execução parece estar em curso ({conteudo}, há {idade / 60:.0f} min). "
                f"Se não estiver, apague {TRAVA}.")
            return False
        log(f"Trava órfã encontrada ({idade / 3600:.1f} h) — ignorando.")
    TRAVA.write_text(f"pid={os.getpid()} inicio={datetime.now():%Y-%m-%d %H:%M:%S}",
                     encoding="utf-8")
    return True


def liberar_trava():
    TRAVA.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# execução de uma etapa
# ---------------------------------------------------------------------------
def executar(etapa: Etapa, log: Log) -> bool:
    faltando = [p for p in etapa.exige if not p.exists()]
    if faltando:
        log(f"[{etapa.nome}] ERRO: arquivo de entrada ausente -> {faltando[0]}")
        return False

    espera = etapa.espera
    for tentativa in range(1, etapa.tentativas + 1):
        if etapa.tentativas > 1:
            log(f"[{etapa.nome}] tentativa {tentativa}/{etapa.tentativas}")
        try:
            proc = subprocess.Popen(
                etapa.comando, cwd=str(SCRIPTS),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace",
                env={**os.environ, "PRONAF_RAIZ": str(ROOT), "PYTHONIOENCODING": "utf-8"},
            )
            try:
                for linha in proc.stdout:
                    log.bruto("    " + linha.rstrip())
                proc.wait(timeout=etapa.timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                log(f"[{etapa.nome}] ERRO: excedeu o tempo limite de {etapa.timeout}s")
                return False
            if proc.returncode == 0:
                return True
            log(f"[{etapa.nome}] terminou com código {proc.returncode}")
        except FileNotFoundError:
            log(f"[{etapa.nome}] ERRO: executável não encontrado ({etapa.comando[0]}). "
                "Node.js e Python precisam estar no PATH.")
            return False
        if tentativa < etapa.tentativas:
            log(f"[{etapa.nome}] aguardando {espera}s antes de nova tentativa")
            time.sleep(espera)
            espera *= 2
    return False


def checar_prerrequisitos(etapas: list, log: Log) -> bool:
    ok = True
    if any(e.rodar and e.comando[0] == "node" for e in etapas):
        try:
            v = subprocess.run(["node", "--version"], capture_output=True, text=True, timeout=30)
            log(f"Node.js         : {v.stdout.strip() or 'desconhecido'}")
        except (FileNotFoundError, subprocess.TimeoutExpired):
            log("ERRO: Node.js não encontrado no PATH — as etapas de download não rodam.")
            ok = False
    log(f"Python          : {sys.version.split()[0]} ({sys.executable})")
    for e in etapas:
        if e.rodar and not Path(e.comando[1]).exists():
            log(f"ERRO: script ausente para a etapa '{e.nome}': {e.comando[1]}")
            ok = False
    return ok


def totais_da_carga(log: Log):
    db = DATA / "dw_pronaf.db"
    if not db.exists():
        return None
    try:
        con = sqlite3.connect(db)
        n, v = con.execute("SELECT SUM(qtde_contratos), SUM(valor_contratado) "
                           "FROM fato_contratacao_pronaf").fetchone()
        ev = con.execute("SELECT SUM(qtde_reconhecimentos) FROM fato_evento_climatico").fetchone()[0]
        con.close()
        v_fmt = f"{v:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")
        return f"{int(n):,}".replace(",", "."), f"R$ {v_fmt}", f"{int(ev or 0):,}".replace(",", ".")
    except (sqlite3.Error, TypeError) as e:
        log(f"[aviso] não foi possível ler os totais do DW: {e}")
        return None


# ---------------------------------------------------------------------------
# principal
# ---------------------------------------------------------------------------
def main() -> int:
    inicio = datetime.now()
    log = Log(LOGS / f"pipeline_{inicio:%Y%m%d_%H%M%S}.log")
    log("=" * 78)
    log("PIPELINE PRONAF RS — download incremental + ETL")
    log("=" * 78)
    log(f"Raiz do projeto : {ROOT}")
    log(f"Log desta carga : {log.caminho}")
    log(f"Modo            : {'SIMULAÇÃO (--dry-run)' if DRY_RUN else 'execução real'}"
        f"{' | --sem-download' if SEM_DOWNLOAD else ''}"
        f"{' | --carga-completa' if CARGA_COMPLETA else ''}")
    log(f"Atualização     : últimos {MESES} meses -> arquivos anuais "
        f"{anos_dos_ultimos_meses(MESES)}")

    etapas = montar_etapas()
    log("")
    log("Plano de execução:")
    for i, e in enumerate(etapas, 1):
        situacao = "executa" if e.rodar else f"dispensada ({e.motivo})"
        log(f"  {i}. {e.nome:<14} {situacao:<52} {e.descricao}")
    log("")

    if not checar_prerrequisitos(etapas, log):
        log("Pré-requisitos não atendidos — nada foi executado.")
        log.fechar()
        return 1

    if DRY_RUN:
        log("Comandos que seriam executados:")
        for e in etapas:
            if e.rodar:
                log.bruto("    " + " ".join(f'"{c}"' if " " in c else c for c in e.comando))
        log("Simulação concluída (nenhuma alteração foi feita).")
        log.fechar()
        return 0

    if not adquirir_trava(log):
        log.fechar()
        return 2

    resultados, houve_falha = [], False
    try:
        for e in etapas:
            if not e.rodar:
                resultados.append((e.nome, "DISPENSADA", 0.0))
                continue
            log("-" * 78)
            log(f"ETAPA {e.nome.upper()} — {e.descricao}")
            log("-" * 78)
            t0 = time.time()
            ok = executar(e, log)
            dur = time.time() - t0
            status = "OK" if ok else ("FALHA (opcional)" if e.opcional else "FALHA")
            resultados.append((e.nome, status, dur))
            log(f"[{e.nome}] {status} em {dur / 60:.1f} min")
            if not ok and e.opcional:
                log(f"[{e.nome}] falha não bloqueante: os arquivos anteriores continuam em "
                    "data/raw (o download grava em .part e só substitui ao concluir).")
            if not ok and not e.opcional:
                houve_falha = True
                log("Interrompendo o pipeline.")
                break
    finally:
        liberar_trava()

    fim = datetime.now()
    log("")
    log("=" * 78)
    log("RESUMO DA EXECUÇÃO")
    log("=" * 78)
    for nome, status, dur in resultados:
        log(f"  {nome:<14} {status:<18} {dur / 60:>6.1f} min")
    log(f"  {'TOTAL':<14} {'':<18} {(fim - inicio).total_seconds() / 60:>6.1f} min")

    carga = totais_da_carga(log)
    if carga:
        log(f"Data warehouse  : {carga[0]} contratos | {carga[1]} | {carga[2]} reconhecimentos")
    log("Cluster K-means : " + ("preservado de data/ml/ml_cluster_municipio.csv (ML não é reexecutado)"
                                if ML_CLUSTER.exists() else
                                "ainda não calculado — rode 03_ml_clusters.py uma única vez"))
    log(f"Início / fim    : {inicio:%d/%m/%Y %H:%M:%S} -> {fim:%d/%m/%Y %H:%M:%S}")
    avisos = any(st == "FALHA (opcional)" for _, st, _ in resultados)
    log("PIPELINE CONCLUÍDO COM FALHA" if houve_falha else
        "PIPELINE CONCLUÍDO COM AVISOS (download recente falhou; ETL rodou com os arquivos existentes)"
        if avisos else "PIPELINE CONCLUÍDO COM SUCESSO")
    log.fechar()
    return 1 if houve_falha else 0


if __name__ == "__main__":
    sys.exit(main())
