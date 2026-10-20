#!/usr/bin/env node
/**
 * Baixa (e opcionalmente extrai) os arquivos brutos do SICOR (Banco Central do Brasil):
 *  - Operacoes por estado (1 arquivo por ano)
 *  - Complemento (municipio IBGE das operacoes)
 *  - Tabelas de dominio (descricoes dos codigos)
 *
 * Uso:
 *   cd "D:\Cursos\POS ABI\Projetos Integrados de Analytics\scripts" && node 01_download_sicor.js
 *   node 01_download_sicor.js                     # baixa tudo (pula o que ja existe)
 *   node 01_download_sicor.js --force             # rebaixa/reextrai mesmo o que ja existe
 *   node 01_download_sicor.js --anos 2022-2024    # restringe o intervalo de anos
 *   node 01_download_sicor.js --extrair           # descompacta os .gz apos baixar
 *   node 01_download_sicor.js --extrair --apagar-gz  # descompacta e remove o .gz original
 *   node 01_download_sicor.js --somente-extrair   # nao baixa nada, so extrai os .gz ja presentes
 */

const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const { pipeline } = require("stream/promises");

const BASE_URL = "https://www.bcb.gov.br/htms/sicor";
// Raiz do projeto: a variavel de ambiente PRONAF_RAIZ (definida pelo 00_pipeline.py)
// tem prioridade; sem ela, vale o caminho fixo abaixo, para uso manual.
const RAIZ =
  process.env.PRONAF_RAIZ ||
  "D:\\Cursos\\POS ABI\\Projetos Integrados de Analytics";

const OUTPUT_DIR = path.resolve(RAIZ, "data", "raw", "sicor");
// Onde os .csv descompactados sao gravados. Aponte para OUTPUT_DIR se quiser
// tudo na mesma pasta.
const EXTRACT_DIR = path.resolve(RAIZ, "data", "raw", "sicor");

const ANO_INICIO_PADRAO = 2019;
const ANO_FIM_PADRAO = 2026;

const MAX_TENTATIVAS = 4;
const TIMEOUT_MS = 1_800_000; // 30 min por tentativa
const ESPERA_ENTRE_TENTATIVAS_MS = 5_000;

const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
  Accept: "*/*",
};

function parseArgs(argv) {
  const args = {
    force: false,
    extrair: false,
    apagarGz: false,
    somenteExtrair: false,
    anoInicio: ANO_INICIO_PADRAO,
    anoFim: ANO_FIM_PADRAO,
  };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--force") {
      args.force = true;
    } else if (argv[i] === "--extrair") {
      args.extrair = true;
    } else if (argv[i] === "--apagar-gz") {
      args.apagarGz = true;
      args.extrair = true;
    } else if (argv[i] === "--somente-extrair") {
      args.somenteExtrair = true;
      args.extrair = true;
    } else if (argv[i] === "--anos") {
      const val = argv[i + 1] || "";
      const m = val.match(/^(\d{4})-(\d{4})$/);
      if (m) {
        args.anoInicio = parseInt(m[1], 10);
        args.anoFim = parseInt(m[2], 10);
        i++;
      }
    }
  }
  return args;
}

function montarListaArquivos(anoInicio, anoFim) {
  const arquivos = [];

  for (let ano = anoInicio; ano <= anoFim; ano++) {
    arquivos.push({
      url: `${BASE_URL}/DadosBrutos/SICOR_OPERACAO_BASICA_ESTADO_${ano}.gz`,
      nomeArquivo: `SICOR_OPERACAO_BASICA_ESTADO_${ano}.gz`,
    });
  }

  arquivos.push({
    url: `${BASE_URL}/DadosBrutos/SICOR_COMPLEMENTO_OPERACAO_BASICA.gz`,
    nomeArquivo: "SICOR_COMPLEMENTO_OPERACAO_BASICA.gz",
  });

  // A maioria das tabelas de dominio fica na raiz de /sicor, mas a lista de
  // instituicoes financeiras fica em /sicor/DadosBrutos (confirmado por teste).
  const tabelasDominioRaiz = [
    "Programa.csv",
    "FonteRecursos.csv",
    "CategoriaEmitente.csv",
    "InstrumentoCredito.csv",
    "Empreendimento.csv",
    "SubProgramas.csv",
  ];

  for (const nome of tabelasDominioRaiz) {
    arquivos.push({ url: `${BASE_URL}/${nome}`, nomeArquivo: nome });
  }

  arquivos.push({
    url: `${BASE_URL}/DadosBrutos/SICOR_LISTA_IFS.csv`,
    nomeArquivo: "SICOR_LISTA_IFS.csv",
  });

  return arquivos;
}

function formatarBytes(bytes) {
  if (!bytes || bytes < 0) return "?";
  const unidades = ["B", "KB", "MB", "GB"];
  let i = 0;
  let valor = bytes;
  while (valor >= 1024 && i < unidades.length - 1) {
    valor /= 1024;
    i++;
  }
  return `${valor.toFixed(1)} ${unidades[i]}`;
}

async function baixarArquivo(url, destino) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), TIMEOUT_MS);

  try {
    const resposta = await fetch(url, {
      headers: HEADERS,
      redirect: "follow",
      signal: controller.signal,
    });

    if (!resposta.ok) {
      throw new Error(`HTTP ${resposta.status} ${resposta.statusText}`);
    }
    if (!resposta.body) {
      throw new Error("Resposta sem corpo (body)");
    }

    const tamanhoEsperado = Number(resposta.headers.get("content-length") || 0);
    const tmpDestino = `${destino}.part`;

    const writeStream = fs.createWriteStream(tmpDestino);
    await pipeline(resposta.body, writeStream);

    const tamanhoBaixado = fs.statSync(tmpDestino).size;
    if (tamanhoEsperado > 0 && tamanhoBaixado !== tamanhoEsperado) {
      fs.unlinkSync(tmpDestino);
      throw new Error(
        `Tamanho incompleto: esperado ${formatarBytes(tamanhoEsperado)}, recebido ${formatarBytes(tamanhoBaixado)}`,
      );
    }

    fs.renameSync(tmpDestino, destino);
    return tamanhoBaixado;
  } finally {
    clearTimeout(timeoutId);
  }
}

async function baixarComRetentativas(url, destino, nomeArquivo) {
  for (let tentativa = 1; tentativa <= MAX_TENTATIVAS; tentativa++) {
    try {
      process.stdout.write(`  Tentativa ${tentativa}/${MAX_TENTATIVAS}... `);
      const bytes = await baixarArquivo(url, destino);
      console.log(`OK (${formatarBytes(bytes)})`);
      return true;
    } catch (erro) {
      console.log(`falhou (${erro.message})`);
      if (tentativa < MAX_TENTATIVAS) {
        await new Promise((r) => setTimeout(r, ESPERA_ENTRE_TENTATIVAS_MS));
      } else {
        console.error(
          `  ERRO: nao foi possivel baixar ${nomeArquivo} apos ${MAX_TENTATIVAS} tentativas.`,
        );
        return false;
      }
    }
  }
  return false;
}

/* ------------------------- extracao dos .gz ------------------------- */

// Le os 2 primeiros bytes para confirmar que e mesmo gzip (1f 8b).
// Se o BCB devolver um HTML de erro com nome .gz, isso pega antes de estourar.
function ehGzip(caminho) {
  const fd = fs.openSync(caminho, "r");
  const buf = Buffer.alloc(2);
  try {
    fs.readSync(fd, buf, 0, 2, 0);
  } finally {
    fs.closeSync(fd);
  }
  return buf[0] === 0x1f && buf[1] === 0x8b;
}

// SICOR_OPERACAO_BASICA_ESTADO_2019.gz -> SICOR_OPERACAO_BASICA_ESTADO_2019.csv
function nomeExtraido(nomeArquivo) {
  const semGz = nomeArquivo.replace(/\.gz$/i, "");
  return path.extname(semGz) ? semGz : `${semGz}.csv`;
}

async function extrairGz(origem, destino) {
  const tmp = `${destino}.part`;
  try {
    await pipeline(
      fs.createReadStream(origem),
      zlib.createGunzip(),
      fs.createWriteStream(tmp),
    );
    fs.renameSync(tmp, destino);
  } catch (erro) {
    if (fs.existsSync(tmp)) fs.unlinkSync(tmp);
    throw erro;
  }
  return fs.statSync(destino).size;
}

async function extrairComVerificacao(nomeArquivo, args) {
  const origem = path.join(OUTPUT_DIR, nomeArquivo);
  const destino = path.join(EXTRACT_DIR, nomeExtraido(nomeArquivo));

  if (!fs.existsSync(origem)) {
    console.log(`  Extracao: .gz nao encontrado, pulando.`);
    return false;
  }
  if (!args.force && fs.existsSync(destino)) {
    console.log(`  Extracao: ${path.basename(destino)} ja existe, pulando.`);
    return true;
  }
  if (!ehGzip(origem)) {
    console.error(`  ERRO: ${nomeArquivo} nao parece ser gzip valido.`);
    return false;
  }

  try {
    process.stdout.write(`  Extraindo -> ${path.basename(destino)}... `);
    const bytes = await extrairGz(origem, destino);
    console.log(`OK (${formatarBytes(bytes)})`);
    if (args.apagarGz) {
      fs.unlinkSync(origem);
      console.log(`  .gz original removido.`);
    }
    return true;
  } catch (erro) {
    console.log(`falhou (${erro.message})`);
    return false;
  }
}

/* ------------------------------ main ------------------------------- */

async function main() {
  const args = parseArgs(process.argv.slice(2));

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  if (args.extrair) fs.mkdirSync(EXTRACT_DIR, { recursive: true });

  const arquivos = montarListaArquivos(args.anoInicio, args.anoFim);

  console.log(`Diretorio de destino: ${OUTPUT_DIR}`);
  if (args.extrair) console.log(`Diretorio de extracao: ${EXTRACT_DIR}`);
  console.log(`Total de arquivos: ${arquivos.length}`);
  console.log(`Anos de operacoes: ${args.anoInicio}-${args.anoFim}`);
  console.log("");

  const resultados = {
    sucesso: [],
    falha: [],
    ignorado: [],
    extraido: [],
    falhaExtracao: [],
  };

  for (const [indice, { url, nomeArquivo }] of arquivos.entries()) {
    const destino = path.join(OUTPUT_DIR, nomeArquivo);
    console.log(`[${indice + 1}/${arquivos.length}] ${nomeArquivo}`);

    if (args.somenteExtrair) {
      // pula o download por completo
    } else if (!args.force && fs.existsSync(destino)) {
      console.log("  Ja existe, pulando (use --force para rebaixar).");
      resultados.ignorado.push(nomeArquivo);
    } else {
      const ok = await baixarComRetentativas(url, destino, nomeArquivo);
      if (ok) {
        resultados.sucesso.push(nomeArquivo);
      } else {
        resultados.falha.push(nomeArquivo);
        continue; // sem arquivo baixado, nao ha o que extrair
      }
    }

    if (args.extrair && nomeArquivo.toLowerCase().endsWith(".gz")) {
      const ok = await extrairComVerificacao(nomeArquivo, args);
      if (ok) {
        resultados.extraido.push(nomeArquivo);
      } else {
        resultados.falhaExtracao.push(nomeArquivo);
      }
    }
  }

  console.log("\n===== Resumo =====");
  console.log(`Baixados com sucesso: ${resultados.sucesso.length}`);
  console.log(`Ja existiam (pulados): ${resultados.ignorado.length}`);
  console.log(`Falharam: ${resultados.falha.length}`);
  if (args.extrair) {
    console.log(`Extraidos: ${resultados.extraido.length}`);
    console.log(`Falhas na extracao: ${resultados.falhaExtracao.length}`);
  }
  if (resultados.falha.length > 0) {
    console.log("Arquivos com falha no download:");
    resultados.falha.forEach((f) => console.log(`  - ${f}`));
    process.exitCode = 1;
  }
  if (resultados.falhaExtracao.length > 0) {
    console.log("Arquivos com falha na extracao:");
    resultados.falhaExtracao.forEach((f) => console.log(`  - ${f}`));
    process.exitCode = 1;
  }
}

main().catch((erro) => {
  console.error("Erro fatal:", erro);
  process.exitCode = 1;
});
