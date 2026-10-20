# Pipeline de dados — Pronaf RS

Carga automatizada do projeto: **download incremental das fontes + ETL**, disparada **quinzenalmente, às 02:00**, pelo Agendador de Tarefas do Windows. O modelo de machine learning roda **uma única vez**, fora da carga.

```
python 00_pipeline.py          # o que o agendador executa
python 03_ml_clusters.py       # execução única, depois do primeiro ETL
```

---

## Estrutura

```
Projetos Integrados de Analytics\
├─ scripts\
│   ├─ 00_pipeline.py            ← orquestrador (download incremental + ETL)
│   ├─ executar_pipeline.bat     ← ponto de entrada do agendador
│   ├─ agendar_pipeline.ps1      ← cria / testa / consulta / remove a tarefa agendada
│   ├─ 01_download_sicor.js      ← extração SICOR (Bacen)
│   ├─ 01_download_atlas.mjs     ← extração Atlas Digital de Desastres
│   ├─ 01_download_ibge.js       ← extração municípios (IBGE)
│   ├─ 02_etl_pronaf_rs.py       ← transformação e carga (DW em CSV + SQLite)
│   └─ 03_ml_clusters.py         ← K-means, treino 80% / teste 20% (execução única)
└─ data\
    ├─ raw\{sicor,atlas_digital,ibge}\
    ├─ staging\                  ← operação a operação (gerado pelo ETL)
    ├─ dw\                       ← esquema estrela em CSV pt-BR (lido pelo Power BI)
    ├─ ml\                       ← resultados do modelo (gerado uma vez pelo 03)
    ├─ logs\                     ← pipeline_AAAAMMDD_HHMMSS.log, ultima_execucao.txt
    └─ dw_pronaf.db
```

Pré-requisitos: Node.js 18+ e Python 3.10+ no PATH; `pip install pandas openpyxl requests scikit-learn matplotlib`.

| Prefixo | Fase | Na carga agendada? |
|---|---|---|
| `00_` | Orquestração | sim |
| `01_` | Extração | sim (incremental) |
| `02_` | Transformação e carga | sim |
| `03_` | Análise avançada (ML) | **não** — execução única |

---

## O que a carga agendada faz

```
 Agendador do Windows ──► executar_pipeline.bat ──► 00_pipeline.py
                                                        │
   1. sicor_base     baixa só arquivos 2019–2025 AUSENTES (na prática, só na 1ª carga)
   2. sicor_recente  rebaixa o(s) arquivo(s) anual(is) dos ÚLTIMOS 3 MESES + complemento + domínios
   3. atlas          só se data\raw\atlas_digital estiver vazia
   4. ibge           só se data\raw\ibge estiver vazia
   5. etl            staging → dimensões → fatos → data\dw\*.csv → dw_pronaf.db
                     (relê o cluster já calculado em data\ml e o grava na dim_municipio)
```

**Por que "últimos 3 meses" vira "arquivo anual".** O Bacen não publica arquivos mensais: ele reescreve o arquivo do ano corrente a cada divulgação. Os meses recentes só chegam rebaixando o arquivo anual que os contém — em setembro/2026, o de 2026; em janeiro/2027, os de 2026 e 2027. A janela é configurável com `--meses N`.

**Por que os resultados do dashboard não mudam.** O ETL analisa uma janela fechada — safras 2019/20 a 2024/25 (jul/2019 a jun/2025) — e ignora arquivos de anos posteriores. O download recente mantém a camada raw atualizada e demonstra a rotina incremental, mas não altera os números do painel. Para ampliar a análise, basta estender `DATA_FIM` no `02_etl_pronaf_rs.py` e `ANO_FIM` no `00_pipeline.py`.

**Falhas.** Se o download recente falhar (site do Bacen fora do ar, por exemplo), a carga registra um aviso e segue com os arquivos existentes — o download grava em `.part` e só substitui o arquivo antigo ao concluir. Se faltar arquivo obrigatório da janela, ou se o ETL falhar, a carga é interrompida com código 1.

---

## Opções

| Comando | Efeito |
|---|---|
| `python 00_pipeline.py` | carga normal (incremental) |
| `--dry-run` | mostra o plano e os comandos, não executa nada |
| `--sem-download` | só o ETL, com os arquivos já presentes |
| `--carga-completa` | rebaixa toda a janela 2019–2025 do SICOR |
| `--meses 6` | muda a janela do download incremental (padrão 3) |
| `--raiz "<caminho>"` | raiz do projeto (também aceita a variável `PRONAF_RAIZ`) |

Códigos de saída: `0` sucesso · `1` etapa obrigatória falhou · `2` outra execução em curso (trava em `data\logs\pipeline.lock`; travas com mais de 6 h são ignoradas).

---

## Agendamento (Windows)

PowerShell **como Administrador**, dentro de `scripts\`:

```powershell
.\agendar_pipeline.ps1                                            # padrão: a cada 2 semanas, domingo às 02:00
.\agendar_pipeline.ps1 -Frequencia Quinzenal -DiaSemana Quarta -Hora "02:00"
.\agendar_pipeline.ps1 -Frequencia Diaria -Hora "02:00"
.\agendar_pipeline.ps1 -Frequencia Semanal -DiaSemana Domingo -Hora "02:00"
.\agendar_pipeline.ps1 -Frequencia Mensal -DiaMes 5 -Hora "03:00"
.\agendar_pipeline.ps1 -Testar        # dispara agora
.\agendar_pipeline.ps1 -Status        # última execução, resultado e próximo disparo
.\agendar_pipeline.ps1 -Remover
```

**Por que quinzenal.** A análise trabalha com uma janela fechada (ver acima) e o download recente rebaixa o arquivo anual inteiro; rodar a cada 14 dias mantém a camada raw em dia sem repetir esse download todas as noites. A quinzena é contada a partir da data em que a tarefa é registrada; `-Status` mostra a data exata do próximo disparo. Para reagendar, basta rodar o script de novo com outros parâmetros (a tarefa existente é substituída).

A tarefa roda com o usuário logado (sessão bloqueada serve). Se o computador estiver desligado no horário, a execução acontece assim que ele for ligado; há 2 retentativas e limite de 6 h.

Equivalente em Linux/macOS (`crontab -e`, a cada 2 semanas, domingo às 02:00). O cron não tem intervalo de semanas, então a linha dispara todo domingo e só segue nas semanas pares:

```cron
0 2 * * 0 [ $(( $(date +\%s) / 604800 \% 2 )) -eq 0 ] && cd /caminho/projeto/scripts && python3 00_pipeline.py --raiz /caminho/projeto
```

---

## Machine learning — execução única

`03_ml_clusters.py` segmenta os municípios por K-means (k = 5) a partir de nove indicadores de crédito e de exposição climática:

1. elegíveis: municípios com pelo menos 100 contratos no período;
2. divisão **80% treino / 20% teste**, estratificada por região intermediária (semente 42);
3. padronização e K-means ajustados **somente no treino**;
4. os municípios de teste são atribuídos ao centróide mais próximo, sem terem sido vistos pelo modelo;
5. avaliação no teste: silhueta, distância média ao centróide, concordância com um modelo de referência ajustado em 100% dos dados e ARI.

Saídas em `data\ml\`: `ml_cluster_municipio.csv`, `ml_metricas.csv`, `ml_perfil_clusters.csv`, `ml_silhueta_k.csv`, `ml_clusters.png`, `modelo_kmeans.joblib`; e a coluna `cluster_kmeans` da `dim_municipio`. Uma segunda execução é recusada; para treinar de novo use `--refazer` (os clusters podem mudar).

O ETL lê `data\ml\ml_cluster_municipio.csv` a cada carga e regrava o cluster na `dim_municipio`; por isso o ML não precisa — e não deve — estar na carga agendada.

---

## Diagnóstico

| Sintoma | Causa provável |
|---|---|
| `executável não encontrado (node)` | Node fora do PATH do usuário da tarefa — use o caminho completo no `.bat` |
| `Outra execução parece estar em curso` | trava presente; confirme e apague `data\logs\pipeline.lock` |
| `arquivo de entrada ausente -> ...SICOR_OPERACAO_BASICA_ESTADO_2019.gz` | base histórica incompleta; rode sem `--sem-download` |
| `CONCLUÍDO COM AVISOS` | o download recente falhou; o ETL usou os arquivos existentes |
| `cluster_kmeans` = "Não calculado" no painel | o `03_ml_clusters.py` ainda não foi executado |
| Download do Atlas falha com HTTP 403 | o site mudou; baixe o CSV à mão e rode `node 01_download_atlas.mjs --file <arquivo>` |
