/**
 * Baixa os municípios do RS (UF 43) na API de Localidades do IBGE
 * e grava os arquivos na camada raw do projeto.
 *
 * Requisitos: Node.js 18+ (fetch nativo)
 * Uso: node 01_download_ibge.js
 */

const fs = require("fs");
const path = require("path");

const URL =
  "https://servicodados.ibge.gov.br/api/v1/localidades/estados/43/municipios?view=nivelado";

// PRONAF_RAIZ e definida pelo orquestrador (00_pipeline.py); sem ela, vale o caminho fixo.
const RAIZ = process.env.PRONAF_RAIZ ||
  "D:\\Cursos\\POS ABI\\Projetos Integrados de Analytics";

const DESTINO = path.join(RAIZ, "data", "raw", "ibge");

const NOME_BASE = "municipios_rs";
const SEPARADOR = ";";   // ";" abre direto no Excel pt-BR; troque por "," se preferir
const SALVAR_JSON = true; // guarda o payload original junto do CSV

/** Converte um array de objetos planos em texto CSV. */
function paraCsv(registros, separador) {
  // União das chaves, preservando a ordem de aparição
  const colunas = [];
  for (const registro of registros) {
    for (const chave of Object.keys(registro)) {
      if (!colunas.includes(chave)) colunas.push(chave);
    }
  }

  const escapar = (valor) => {
    if (valor === null || valor === undefined) return "";
    const texto = String(valor);
    return /["\r\n]|[;,\t]/.test(texto)
      ? `"${texto.replace(/"/g, '""')}"`
      : texto;
  };

  const linhas = [colunas.join(separador)];
  for (const registro of registros) {
    linhas.push(colunas.map((c) => escapar(registro[c])).join(separador));
  }

  return linhas.join("\r\n");
}

async function main() {
  fs.mkdirSync(DESTINO, { recursive: true });

  console.log(`Baixando: ${URL}`);
  const resposta = await fetch(URL, {
    headers: { Accept: "application/json" },
  });

  if (!resposta.ok) {
    throw new Error(`Falha HTTP ${resposta.status} - ${resposta.statusText}`);
  }

  const dados = await resposta.json();

  if (!Array.isArray(dados) || dados.length === 0) {
    throw new Error("A resposta não trouxe registros.");
  }

  if (SALVAR_JSON) {
    const caminhoJson = path.join(DESTINO, `${NOME_BASE}.json`);
    fs.writeFileSync(caminhoJson, JSON.stringify(dados, null, 2), "utf8");
    console.log(`JSON salvo em: ${caminhoJson}`);
  }

  const caminhoCsv = path.join(DESTINO, `${NOME_BASE}.csv`);
  // BOM UTF-8 para os acentos não quebrarem no Excel
  fs.writeFileSync(caminhoCsv, "\uFEFF" + paraCsv(dados, SEPARADOR), "utf8");

  console.log(`CSV salvo em: ${caminhoCsv}`);
  console.log(`Registros: ${dados.length}`);
  console.log(`Colunas: ${Object.keys(dados[0]).join(", ")}`);
}

main().catch((erro) => {
  console.error("Erro na extração:", erro.message);
  process.exit(1);
});
