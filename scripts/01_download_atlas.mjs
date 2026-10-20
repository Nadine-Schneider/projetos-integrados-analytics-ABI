#!/usr/bin/env node
/**
 * Baixa a Base de Dados Completa (CSV) do Atlas Digital de Desastres,
 * filtra as ocorrências do Rio Grande do Sul entre 2019 e 2025 (por Data_Evento)
 * e salva em data/raw/atlas_rs.csv
 *
 * Requer Node 18+ (usa fetch nativo). Sem dependências externas.
 *
 * Uso:
 *   node 01_download_atlas.mjs                  # baixa da internet
 *   node 01_download_atlas.mjs --uf SC          # outro estado
 *   node 01_download_atlas.mjs --de 2015 --ate 2025   # outro período
 *   node 01_download_atlas.mjs --data-col "Data Ocorrencia"  # outra coluna de data
 *   node 01_download_atlas.mjs --file bd.csv    # usa um CSV já baixado
 *   node 01_download_atlas.mjs --out saida.csv  # outro destino
 *   node 01_download_atlas.mjs --bom            # BOM UTF-8 (Excel PT-BR)
 */

import fs from "node:fs/promises";
import path from "node:path";

// PRONAF_RAIZ e definida pelo orquestrador (00_pipeline.py); sem ela, vale o caminho fixo.
const RAIZ = process.env.PRONAF_RAIZ ||
  "D:\\Cursos\\POS ABI\\Projetos Integrados de Analytics";

const PAGINA = "https://atlasdigital.mdr.gov.br/paginas/downloads.xhtml";
const DELIM = ";";

// ---------- argumentos ----------
function parseArgs(argv) {
  const args = {
    uf: "RS",
    de: 2019,
    ate: 2025,
    dataCol: "Data_Evento",
    out: path.join(RAIZ, "data", "raw", "atlas_digital", "atlas_rs.csv"),
    file: null,
    bom: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--uf") args.uf = argv[++i]?.toUpperCase();
    else if (a === "--de") args.de = Number(argv[++i]);
    else if (a === "--ate") args.ate = Number(argv[++i]);
    else if (a === "--data-col") args.dataCol = argv[++i];
    else if (a === "--out") args.out = argv[++i];
    else if (a === "--file") args.file = argv[++i];
    else if (a === "--bom") args.bom = true;
  }
  return args;
}

// ---------- download ----------
/** Lê a página de downloads e devolve a URL do CSV (o nome muda a cada versão). */
async function descobrirUrlCsv() {
  const res = await fetch(PAGINA, { headers: { "User-Agent": "Mozilla/5.0" } });
  if (!res.ok)
    throw new Error(`Página de downloads retornou HTTP ${res.status}`);
  const html = await res.text();

  const hrefs = [...html.matchAll(/href\s*=\s*["']([^"']+\.csv)["']/gi)].map(
    (m) => m[1],
  );
  if (hrefs.length === 0) {
    throw new Error(
      "Nenhum link .csv encontrado na página — o layout do site pode ter mudado.",
    );
  }
  // prioriza o arquivo consolidado da base completa
  const escolhido = hrefs.find((h) => /consolidado/i.test(h)) ?? hrefs[0];
  return new URL(escolhido, PAGINA).href;
}

/** Baixa bytes e decodifica: tenta UTF-8 estrito, cai para windows-1252 (ANSI). */
async function baixarTexto(url) {
  const res = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0" } });
  if (!res.ok) throw new Error(`Download falhou: HTTP ${res.status} em ${url}`);
  return decodificar(Buffer.from(await res.arrayBuffer()));
}

function decodificar(buf) {
  try {
    return new TextDecoder("utf-8", { fatal: true })
      .decode(buf)
      .replace(/^\uFEFF/, "");
  } catch {
    return new TextDecoder("windows-1252").decode(buf);
  }
}

// ---------- CSV ----------
/** Parser que respeita aspas, aspas duplicadas ("") e quebras de linha dentro do campo. */
function parseCsv(texto, delim = DELIM) {
  const linhas = [];
  let campo = "";
  let linha = [];
  let dentroAspas = false;

  for (let i = 0; i < texto.length; i++) {
    const c = texto[i];
    if (dentroAspas) {
      if (c === '"') {
        if (texto[i + 1] === '"') {
          campo += '"';
          i++;
        } else dentroAspas = false;
      } else campo += c;
      continue;
    }
    if (c === '"') dentroAspas = true;
    else if (c === delim) {
      linha.push(campo);
      campo = "";
    } else if (c === "\n" || c === "\r") {
      if (c === "\r" && texto[i + 1] === "\n") i++;
      linha.push(campo);
      campo = "";
      if (linha.length > 1 || linha[0] !== "") linhas.push(linha);
      linha = [];
    } else campo += c;
  }
  if (campo !== "" || linha.length) {
    linha.push(campo);
    linhas.push(linha);
  }
  return linhas;
}

function escapar(v, delim = DELIM) {
  const s = String(v ?? "");
  return /["\n\r]/.test(s) || s.includes(delim)
    ? `"${s.replace(/"/g, '""')}"`
    : s;
}

const UFS = new Set([
  "AC",
  "AL",
  "AP",
  "AM",
  "BA",
  "CE",
  "DF",
  "ES",
  "GO",
  "MA",
  "MT",
  "MS",
  "MG",
  "PA",
  "PB",
  "PR",
  "PE",
  "PI",
  "RJ",
  "RN",
  "RS",
  "RO",
  "RR",
  "SC",
  "SP",
  "SE",
  "TO",
]);

/** "Data_Evento", "DATA EVENTO", "Data  Evento" → "data evento" */
const normalizar = (s) =>
  String(s ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[_\s]+/g, " ")
    .trim()
    .toLowerCase();

/** Acha uma coluna pelo cabeçalho, testando vários nomes possíveis. */
function acharColuna(cabecalho, ...nomes) {
  const alvos = nomes.map(normalizar);
  return cabecalho.findIndex((h) => alvos.includes(normalizar(h)));
}

/**
 * Extrai o ano de um valor de data. A base mistura formatos entre versões,
 * então cobrimos ISO, dd/mm/aaaa e serial do Excel.
 */
function extrairAno(valor) {
  const s = String(valor ?? "").trim();
  if (!s) return null;

  let m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/); // 2024-05-03
  if (m) return Number(m[1]);

  m = s.match(/^(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})/); // 03/05/2024
  if (m) return Number(m[3]);

  if (/^\d{5}$/.test(s)) {
    // serial do Excel
    const d = new Date(Date.UTC(1899, 11, 30) + Number(s) * 86400000);
    const ano = d.getUTCFullYear();
    if (ano >= 1900 && ano <= 2100) return ano;
  }

  m = s.match(/\b(19\d{2}|20\d{2})\b/); // último recurso
  return m ? Number(m[1]) : null;
}

/**
 * Acha a coluna de UF pelo cabeçalho; se não achar, procura a coluna cujos
 * valores são siglas de estado. Isso evita quebrar se o nome mudar de versão.
 */
function acharColunaUf(cabecalho, linhas) {
  const porNome = acharColuna(
    cabecalho,
    "UF",
    "Sigla_UF",
    "SG_UF",
    "Estado",
    "UF_Ocorrencia",
  );
  if (porNome !== -1) return porNome;

  const amostra = linhas.slice(0, 200);
  for (let c = 0; c < cabecalho.length; c++) {
    const vals = amostra
      .map((l) =>
        String(l[c] ?? "")
          .trim()
          .toUpperCase(),
      )
      .filter(Boolean);
    if (vals.length >= 10 && vals.every((v) => UFS.has(v))) return c;
  }
  return -1;
}

// ---------- principal ----------
async function main() {
  const args = parseArgs(process.argv.slice(2));

  let texto;
  if (args.file) {
    texto = decodificar(await fs.readFile(args.file));
    console.log(`Lendo arquivo local: ${args.file}`);
  } else {
    const url = await descobrirUrlCsv();
    console.log(`Baixando: ${url}`);
    texto = await baixarTexto(url);
  }

  const linhas = parseCsv(texto);
  if (linhas.length < 2) throw new Error("CSV vazio ou ilegível.");

  const [cabecalho, ...dados] = linhas;
  const colUf = acharColunaUf(cabecalho, dados);
  if (colUf === -1) {
    throw new Error(
      `Coluna de UF não encontrada. Cabeçalho: ${cabecalho.join(" | ")}`,
    );
  }
  console.log(`Coluna de UF: "${cabecalho[colUf]}" (índice ${colUf})`);

  const colData = acharColuna(
    cabecalho,
    args.dataCol,
    "Data_Evento",
    "Data",
    "Data_Ocorrencia",
  );
  if (colData === -1) {
    throw new Error(
      `Coluna de data "${args.dataCol}" não encontrada. Cabeçalho: ${cabecalho.join(" | ")}`,
    );
  }
  console.log(`Coluna de data: "${cabecalho[colData]}" (índice ${colData})`);

  let semData = 0;
  const filtradas = dados.filter((l) => {
    if (
      String(l[colUf] ?? "")
        .trim()
        .toUpperCase() !== args.uf
    )
      return false;
    const ano = extrairAno(l[colData]);
    if (ano === null) {
      semData++;
      return false;
    }
    return ano >= args.de && ano <= args.ate;
  });

  if (semData) {
    console.warn(
      `Aviso: ${semData} linha(s) de ${args.uf} descartadas por data ilegível/vazia.`,
    );
  }
  if (filtradas.length === 0) {
    console.warn(
      `Aviso: nenhuma linha com UF = ${args.uf} entre ${args.de} e ${args.ate}.`,
    );
  }

  const saida = [cabecalho, ...filtradas]
    .map((l) => l.map((v) => escapar(v)).join(DELIM))
    .join("\n");

  await fs.mkdir(path.dirname(args.out), { recursive: true });
  await fs.writeFile(
    args.out,
    (args.bom ? "\uFEFF" : "") + saida + "\n",
    "utf8",
  );

  console.log(
    `${filtradas.length} de ${dados.length} linhas ` +
      `(${args.uf}, ${args.de}–${args.ate}) → ${args.out}`,
  );
}

main().catch((e) => {
  console.error("Erro:", e.message);
  process.exit(1);
});
