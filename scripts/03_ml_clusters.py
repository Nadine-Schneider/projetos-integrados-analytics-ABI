# -*- coding: utf-8 -*-
"""
03_ml_clusters.py — Segmentação K-means dos municípios do RS (RF-04), com treino 80% / teste 20%.

EXECUÇÃO ÚNICA. Este script não faz parte da carga agendada (00_pipeline.py). Ele é rodado uma
vez, depois do primeiro ETL; o resultado fica gravado em data/ml/ e o ETL o reaproveita em todas
as cargas seguintes. Uma segunda execução é recusada, a menos que se passe --refazer.

Protocolo:
    1. features por município a partir do DW (as mesmas 9 da versão anterior);
    2. elegíveis = municípios com >= 100 contratos no período;
    3. divisão aleatória e estratificada por região intermediária: 80% treino, 20% teste (semente 42);
    4. padronização (StandardScaler) e K-means AJUSTADOS SÓ NO TREINO;
    5. os municípios de teste são atribuídos ao centróide mais próximo (predict), sem que o modelo
       os tenha visto;
    6. avaliação no teste — os resultados vão para o dashboard:
         - silhueta no treino x silhueta no teste (o teste deve ficar no mesmo patamar);
         - distância média ao centróide no treino x no teste (razão perto de 1 = generaliza);
         - concordância no teste: % dos municípios de teste que recebem o mesmo cluster que
           receberiam de um modelo de referência ajustado com 100% dos dados (+ ARI);
         - distribuição dos municípios por cluster no treino x no teste.

Entrada : data/dw_pronaf.db (gerado pelo 02_etl_pronaf_rs.py)
Saídas  : data/ml/ml_cluster_municipio.csv  -> 1 linha por município: conjunto (Treino/Teste), cluster,
                                               distância ao centróide, PC1/PC2, features
          data/ml/ml_metricas.csv           -> métricas treino x teste (cartões/tabela do dashboard)
          data/ml/ml_perfil_clusters.csv    -> perfil médio e distribuição treino x teste por cluster
          data/ml/ml_silhueta_k.csv         -> silhueta (treino) para k = 3..7
          data/ml/ml_clusters.png           -> figura do relatório
          data/ml/modelo_kmeans.joblib      -> scaler + K-means treinados (modelo persistido)
          data/dw/dim_municipio.csv + SQLite -> coluna cluster_kmeans preenchida
          SQLite: tabelas ml_cluster_municipio, ml_metricas, ml_perfil_clusters

Uso:
    pip install scikit-learn matplotlib
    python 03_ml_clusters.py [--raiz "D:\\...\\Projetos Integrados de Analytics"] [--k 5 | --k auto] [--refazer]
"""
from pathlib import Path
import sqlite3
import sys

import joblib
import numpy as np
import pandas as pd
from scipy.optimize import linear_sum_assignment
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import adjusted_rand_score, silhouette_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# ----------------------------------------------------------------------------
# configuração
# ----------------------------------------------------------------------------
def descobrir_raiz():
    if "--raiz" in sys.argv:
        return Path(sys.argv[sys.argv.index("--raiz") + 1]).expanduser().resolve()
    aqui = Path(__file__).resolve().parent
    for cand in (aqui, *aqui.parents):
        if (cand / "data" / "dw_pronaf.db").exists():
            return cand
    sys.exit("[ERRO] data/dw_pronaf.db não encontrado — rode o ETL ou use --raiz")


ROOT = descobrir_raiz()
DB, DW, ML = ROOT / "data" / "dw_pronaf.db", ROOT / "data" / "dw", ROOT / "data" / "ml"
SAIDA_PRINCIPAL = ML / "ml_cluster_municipio.csv"
CSV_PT = dict(sep=";", decimal=",", encoding="utf-8-sig", index=False, float_format="%.4f")

# k: a silhueta máxima costuma cair em k=3, mas k=5 separa o cluster dominante (~80% do valor) em
# dois segmentos com perfil comercial distinto; a diferença de silhueta é pequena e todos os valores
# são modestos, típico de dados socioeconômicos. --k auto deixa a silhueta (no treino) decidir.
K_PADRAO = 5
_arg_k = sys.argv[sys.argv.index("--k") + 1] if "--k" in sys.argv else str(K_PADRAO)
K_FIXO = None if _arg_k == "auto" else int(_arg_k)
K_RANGE = range(3, 8)
SEED = 42
PCT_TESTE = 0.20
MIN_CONTRATOS = 100          # municípios com menos contratos no período ficam fora do modelo
LOG_FEATURES = ("ticket_medio", "prejuizo_por_contrato")   # cauda longa -> log10 antes de padronizar

FEATURES = {  # nome -> descrição (vai para o relatório)
    "log_valor_safra": "log10 do valor médio contratado por safra (R$) — tamanho do mercado",
    "ticket_medio": "valor contratado ÷ contratos (R$)",
    "pct_custeio": "participação do custeio no valor",
    "pct_pecuaria": "participação da atividade pecuária no valor",
    "taxa_media": "taxa de juros média ponderada (% a.a.)",
    "pct_cooperativa": "participação das cooperativas de crédito (Sicredi/Cresol/Unicred) no valor",
    "cv_valor_safra": "coef. de variação do valor entre safras — volatilidade da demanda",
    "safras_afetadas": "nº de safras (0–6) com reconhecimento de seca ou excesso hídrico",
    "prejuizo_por_contrato": "prejuízo agropecuário declarado (Atlas) ÷ contratos (R$)",
}


# ----------------------------------------------------------------------------
# 1. features por município (idêntico à versão anterior)
# ----------------------------------------------------------------------------
def montar_features(con):
    q = """
    SELECT m.cd_municipio, m.nome_municipio, m.regiao_intermediaria, t.ano_safra,
           SUM(f.qtde_contratos) contratos, SUM(f.valor_contratado) valor,
           SUM(f.vl_taxa_ponderada) vtp,
           SUM(CASE WHEN fi.descricao = 'Custeio' THEN f.valor_contratado ELSE 0 END) v_custeio,
           SUM(CASE WHEN a.descricao LIKE 'PECU%' THEN f.valor_contratado ELSE 0 END) v_pec,
           SUM(CASE WHEN UPPER(i.descricao) LIKE '%SICREDI%' OR UPPER(i.descricao) LIKE '%CRESOL%'
                      OR UPPER(i.descricao) LIKE '%UNICRED%' THEN f.valor_contratado ELSE 0 END) v_coop,
           MAX(f.flag_safra_afetada) afetada
    FROM fato_contratacao_pronaf f
    JOIN dim_municipio m USING (sk_municipio)
    JOIN dim_tempo t USING (sk_tempo)
    JOIN dim_finalidade fi USING (sk_finalidade)
    JOIN dim_atividade a USING (sk_atividade)
    JOIN dim_instituicao_financeira i USING (sk_instituicao_financeira)
    WHERE m.cd_municipio NOT IN ('ND', 'FORA_RS')
    GROUP BY 1, 2, 3, 4"""
    s = pd.read_sql(q, con)
    prej = pd.read_sql("""SELECT cd_municipio, SUM(prejuizo_agricultura + prejuizo_pecuaria) prejuizo
                          FROM fato_evento_climatico GROUP BY 1""", con)
    prej["cd_municipio"] = prej["cd_municipio"].astype(str).str.zfill(7)
    s["cd_municipio"] = s["cd_municipio"].astype(str).str.zfill(7)

    g = s.groupby(["cd_municipio", "nome_municipio", "regiao_intermediaria"])
    m = g.agg(contratos=("contratos", "sum"), valor=("valor", "sum"), vtp=("vtp", "sum"),
              v_custeio=("v_custeio", "sum"), v_pec=("v_pec", "sum"), v_coop=("v_coop", "sum"),
              n_safras=("ano_safra", "nunique"), safras_afetadas=("afetada", "sum"),
              valor_medio_safra=("valor", "mean"), valor_std_safra=("valor", "std")).reset_index()
    m = m.merge(prej, on="cd_municipio", how="left").fillna({"prejuizo": 0.0})

    m["log_valor_safra"] = np.log10(m["valor_medio_safra"].clip(lower=1))
    m["ticket_medio"] = m["valor"] / m["contratos"]
    m["pct_custeio"] = m["v_custeio"] / m["valor"]
    m["pct_pecuaria"] = m["v_pec"] / m["valor"]
    m["taxa_media"] = m["vtp"] / m["valor"]
    m["pct_cooperativa"] = m["v_coop"] / m["valor"]
    m["cv_valor_safra"] = (m["valor_std_safra"] / m["valor_medio_safra"]).fillna(0)
    m["prejuizo_por_contrato"] = m["prejuizo"] / m["contratos"]
    return m


def matriz(df):
    X = df[list(FEATURES)].to_numpy(dtype=float)
    for i, f in enumerate(FEATURES):
        if f in LOG_FEATURES:
            X[:, i] = np.log10(X[:, i] + 1)
    return X


# ----------------------------------------------------------------------------
# 2. divisão treino/teste, escolha de k, ajuste no treino e avaliação no teste
# ----------------------------------------------------------------------------
def dividir(eleg):
    try:
        tr, te = train_test_split(eleg, test_size=PCT_TESTE, random_state=SEED,
                                  stratify=eleg["regiao_intermediaria"])
        estrat = "estratificada por região intermediária"
    except ValueError:      # alguma região com município demais de menos para estratificar
        tr, te = train_test_split(eleg, test_size=PCT_TESTE, random_state=SEED)
        estrat = "aleatória simples"
    return tr.copy(), te.copy(), estrat


def concordancia(rotulos_modelo, rotulos_ref, k):
    """% de acerto após casar os rótulos dos dois modelos (os números dos clusters são arbitrários)."""
    tab = np.zeros((k, k), dtype=int)
    for a, b in zip(rotulos_modelo, rotulos_ref):
        tab[a, b] += 1
    lin, col = linear_sum_assignment(-tab)
    return tab[lin, col].sum() / len(rotulos_modelo)


def treinar_e_testar(m):
    eleg = m[m["contratos"] >= MIN_CONTRATOS].copy()
    treino, teste, estrat = dividir(eleg)

    scaler = StandardScaler().fit(matriz(treino))            # ajustado só no treino
    Xtr, Xte = scaler.transform(matriz(treino)), scaler.transform(matriz(teste))

    sil_k = {}
    for k in K_RANGE:
        km = KMeans(n_clusters=k, n_init=20, random_state=SEED).fit(Xtr)
        sil_k[k] = silhouette_score(Xtr, km.labels_)
    k = K_FIXO or max(sil_k, key=sil_k.get)

    km = KMeans(n_clusters=k, n_init=50, random_state=SEED).fit(Xtr)
    treino["cluster_id"] = km.labels_
    teste["cluster_id"] = km.predict(Xte)                   # municípios nunca vistos pelo modelo
    treino["distancia_centroide"] = km.transform(Xtr).min(axis=1)
    teste["distancia_centroide"] = km.transform(Xte).min(axis=1)

    # modelo de referência: mesmo protocolo com 100% dos elegíveis (só para medir a concordância)
    sc_ref = StandardScaler().fit(matriz(eleg))
    km_ref = KMeans(n_clusters=k, n_init=50, random_state=SEED).fit(sc_ref.transform(matriz(eleg)))
    ref_teste = km_ref.predict(sc_ref.transform(matriz(teste)))

    n_lab_te = teste["cluster_id"].nunique()
    met = {
        "k": k,
        "estratificacao": estrat,
        "n_treino": len(treino), "n_teste": len(teste),
        "sil_treino": silhouette_score(Xtr, treino["cluster_id"]),
        "sil_teste": (silhouette_score(Xte, teste["cluster_id"])
                      if 2 <= n_lab_te < len(teste) else np.nan),
        "dist_treino": treino["distancia_centroide"].mean(),
        "dist_teste": teste["distancia_centroide"].mean(),
        "concordancia": concordancia(teste["cluster_id"].to_numpy(), ref_teste, k),
        "ari": adjusted_rand_score(ref_teste, teste["cluster_id"]),
    }

    pca = PCA(n_components=2, random_state=SEED).fit(Xtr)
    treino[["pc1", "pc2"]] = pca.transform(Xtr)
    teste[["pc1", "pc2"]] = pca.transform(Xte)
    treino["conjunto_ml"], teste["conjunto_ml"] = "Treino", "Teste"
    return treino, teste, sil_k, met, scaler, km, pca


# ----------------------------------------------------------------------------
# 3. nomes dos clusters a partir do perfil do TREINO (regras simples e explicáveis)
# ----------------------------------------------------------------------------
def nomear(perfil):
    """perfil: DataFrame indexado por cluster_id com as médias das features"""
    z = (perfil - perfil.mean()) / perfil.std(ddof=0)   # posição relativa entre clusters
    nomes = {}
    for cid, r in z.iterrows():
        partes = []
        partes.append("Grande mercado" if r["log_valor_safra"] > 0.5 else
                      "Pequeno mercado" if r["log_valor_safra"] < -0.5 else "Mercado médio")
        if r["ticket_medio"] > 0.5:
            partes.append("ticket alto")
        elif r["ticket_medio"] < -0.5:
            partes.append("ticket baixo")
        if r["pct_pecuaria"] > 0.5:
            partes.append("pecuária")
        elif r["pct_custeio"] > 0.5:
            partes.append("custeio agrícola")
        elif r["pct_custeio"] < -0.5:
            partes.append("investimento")
        if r["safras_afetadas"] > 0.5 or r["prejuizo_por_contrato"] > 0.5:
            partes.append("alta exposição climática")
        elif r["safras_afetadas"] < -0.5:
            partes.append("baixa exposição climática")
        if r["pct_cooperativa"] > 0.5:
            partes.append("cooperativas")
        elif r["pct_cooperativa"] < -0.5:
            partes.append("bancos")
        nomes[cid] = " · ".join(partes)
    vistos = {}
    for cid in sorted(nomes):
        base = nomes[cid]
        vistos[base] = vistos.get(base, 0) + 1
        if vistos[base] > 1:
            nomes[cid] = f"{base} ({vistos[base]})"
    return nomes


# ----------------------------------------------------------------------------
# 4. saídas
# ----------------------------------------------------------------------------
def tabela_metricas(met):
    r = lambda v: None if pd.isna(v) else round(float(v), 4)   # noqa: E731
    linhas = [
        ("Municípios", met["n_treino"], met["n_teste"],
         f"Divisão 80/20 {met['estratificacao']}, semente {SEED}; k = {met['k']}."),
        ("Silhueta", r(met["sil_treino"]), r(met["sil_teste"]),
         "Coesão/separação dos grupos (-1 a 1). Teste no mesmo patamar do treino = a estrutura "
         "se mantém em municípios que o modelo não viu."),
        ("Distância média ao centróide", r(met["dist_treino"]), r(met["dist_teste"]),
         "Em desvios-padrão. Teste próximo do treino = municípios novos se encaixam tão bem "
         "quanto os de treino."),
        ("Razão distância teste/treino", None, r(met["dist_teste"] / met["dist_treino"]),
         "Perto de 1,0 indica ausência de sobreajuste."),
        ("Concordância com modelo de referência (%)", None, r(met["concordancia"] * 100),
         "% dos municípios de teste que recebem o mesmo cluster que receberiam de um modelo "
         "ajustado com 100% dos dados."),
        ("Adjusted Rand Index (ARI)", None, r(met["ari"]),
         "Mesma comparação, corrigida pelo acaso (0 = aleatório, 1 = idêntico)."),
    ]
    df = pd.DataFrame(linhas, columns=["metrica", "treino", "teste", "interpretacao"])

    def txt(v, metrica):   # versão em texto pt-BR, pronta para a tabela do dashboard
        if v is None or pd.isna(v):
            return "—"
        if metrica == "Municípios":
            return f"{int(v)}"
        if metrica.endswith("(%)"):
            return f"{v:.1f}%".replace(".", ",")
        return f"{v:.3f}".replace(".", ",")
    df["treino_txt"] = [txt(v, m) for v, m in zip(df["treino"], df["metrica"])]
    df["teste_txt"] = [txt(v, m) for v, m in zip(df["teste"], df["metrica"])]
    return df[["metrica", "treino", "teste", "treino_txt", "teste_txt", "interpretacao"]]


def figura(todos, sil_k, k, nomes, perfil):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(1, 3, figsize=(18, 5.2), gridspec_kw={"width_ratios": [1, 1.6, 1.1]})
    ax[0].plot(list(sil_k), list(sil_k.values()), marker="o")
    ax[0].axvline(k, ls="--", c="gray")
    ax[0].set_title("Silhueta por k (treino)"); ax[0].set_xlabel("k"); ax[0].set_ylabel("silhueta")

    cores = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    for cid in sorted(nomes):
        c = cores[cid % len(cores)]
        tr = todos[(todos.cluster_id == cid) & (todos.conjunto_ml == "Treino")]
        te = todos[(todos.cluster_id == cid) & (todos.conjunto_ml == "Teste")]
        ax[1].scatter(tr.pc1, tr.pc2, s=14, alpha=.55, color=c, label=f"C{cid}: {nomes[cid]}")
        ax[1].scatter(te.pc1, te.pc2, s=42, marker="x", color=c)
    ax[1].scatter([], [], marker="x", color="k", label="× = município de teste")
    ax[1].set_title(f"K-means (k={k}) treinado em 80% — projeção PCA")
    ax[1].set_xlabel("PC1"); ax[1].set_ylabel("PC2"); ax[1].legend(fontsize=7, loc="best")

    x = np.arange(len(perfil))
    ax[2].bar(x - .2, perfil["pct_municipios_treino"] * 100, .4, label="Treino")
    ax[2].bar(x + .2, perfil["pct_municipios_teste"] * 100, .4, label="Teste")
    ax[2].set_xticks(x, [f"C{c}" for c in perfil["cluster_id"]])
    ax[2].set_ylabel("% dos municípios do conjunto")
    ax[2].set_title("Distribuição por cluster: treino x teste"); ax[2].legend()
    fig.tight_layout(); fig.savefig(ML / "ml_clusters.png", dpi=150)


def main():
    if SAIDA_PRINCIPAL.exists() and "--refazer" not in sys.argv:
        print(f"[ok] O modelo já foi treinado ({SAIDA_PRINCIPAL}).\n"
              "     Este script é de execução única: o ETL reaproveita esse resultado a cada carga.\n"
              "     Para treinar de novo (os clusters podem mudar), rode com --refazer.")
        return

    ML.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB)
    m = montar_features(con)
    treino, teste, sil_k, met, scaler, km, pca = treinar_e_testar(m)
    k = met["k"]
    print("silhueta por k (treino):", {a: round(b, 3) for a, b in sil_k.items()}, "| k:", k)

    nomes = nomear(treino.groupby("cluster_id")[list(FEATURES)].mean())
    todos = pd.concat([treino, teste], ignore_index=True)
    todos["cluster_kmeans"] = todos["cluster_id"].map(lambda c: f"C{c} · {nomes[c]}")

    # --- métricas treino x teste (dashboard)
    tab_met = tabela_metricas(met)
    tab_met.to_csv(ML / "ml_metricas.csv", **CSV_PT)
    print("\n== avaliação treino x teste ==")
    print(tab_met[["metrica", "treino", "teste"]].to_string(index=False))

    # --- perfil por cluster
    perfil = todos.groupby(["cluster_id", "cluster_kmeans"]).agg(
        municipios=("cd_municipio", "size"),
        municipios_treino=("conjunto_ml", lambda s: (s == "Treino").sum()),
        municipios_teste=("conjunto_ml", lambda s: (s == "Teste").sum()),
        valor_total_bi=("valor", lambda v: v.sum() / 1e9), contratos=("contratos", "sum"),
        **{f: (f, "mean") for f in FEATURES}).reset_index()
    perfil["share_valor"] = perfil["valor_total_bi"] / perfil["valor_total_bi"].sum()
    perfil["pct_municipios_treino"] = perfil["municipios_treino"] / met["n_treino"]
    perfil["pct_municipios_teste"] = perfil["municipios_teste"] / met["n_teste"]
    perfil = perfil.sort_values("valor_total_bi", ascending=False)
    perfil.to_csv(ML / "ml_perfil_clusters.csv", **CSV_PT)
    pd.DataFrame({"k": list(sil_k), "silhueta_treino": list(sil_k.values())}) \
        .to_csv(ML / "ml_silhueta_k.csv", **CSV_PT)

    # --- municípios (inclui os que ficaram fora do modelo por terem poucos contratos)
    fora = m[m["contratos"] < MIN_CONTRATOS].copy()
    fora["conjunto_ml"] = "Fora do modelo (< 100 contratos)"
    fora["cluster_kmeans"] = "Sem cluster (poucos contratos)"
    cols = ["cd_municipio", "nome_municipio", "regiao_intermediaria", "conjunto_ml", "cluster_id",
            "cluster_kmeans", "distancia_centroide", "pc1", "pc2", "contratos", "valor", *FEATURES]
    mun = pd.concat([todos, fora], ignore_index=True).reindex(columns=cols)
    mun["cluster_id"] = mun["cluster_id"].astype("Int64")
    mun.sort_values(["conjunto_ml", "cluster_id", "valor"], ascending=[False, True, False]) \
       .to_csv(SAIDA_PRINCIPAL, **CSV_PT)

    joblib.dump({"scaler": scaler, "kmeans": km, "pca": pca, "features": list(FEATURES),
                 "log_features": LOG_FEATURES, "nomes": nomes}, ML / "modelo_kmeans.joblib")

    # --- grava o cluster na dimensão (CSV pt-BR + SQLite), como na versão anterior
    dim = pd.read_csv(DW / "dim_municipio.csv", sep=";", decimal=",", encoding="utf-8-sig", dtype=str)
    mapa = dict(zip(mun["cd_municipio"], mun["cluster_kmeans"]))
    dim["cluster_kmeans"] = dim["cd_municipio"].astype(str).map(mapa).fillna("Não se aplica")
    dim.to_csv(DW / "dim_municipio.csv", **CSV_PT)
    dim.to_sql("dim_municipio", con, if_exists="replace", index=False)
    mun.to_sql("ml_cluster_municipio", con, if_exists="replace", index=False)
    tab_met.to_sql("ml_metricas", con, if_exists="replace", index=False)
    perfil.to_sql("ml_perfil_clusters", con, if_exists="replace", index=False)
    con.close()

    figura(todos, sil_k, k, nomes, perfil)
    print(f"\n[ok] treino: {met['n_treino']} | teste: {met['n_teste']} | "
          f"fora do modelo (< {MIN_CONTRATOS} contratos): {len(fora)}")
    print("[ok] dim_municipio atualizada (CSV + SQLite) — no Power BI: Página Inicial → Atualizar")
    print(f"[ok] saídas em {ML}")


if __name__ == "__main__":
    main()
