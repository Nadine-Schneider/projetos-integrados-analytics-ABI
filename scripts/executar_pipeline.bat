@echo off
REM ============================================================================
REM  executar_pipeline.bat - ponto de entrada da carga automatizada (Windows)
REM
REM  Chamado pelo Agendador de Tarefas (ver agendar_pipeline.ps1). Fixa a raiz do
REM  projeto, roda o 00_pipeline.py (download incremental + ETL) e devolve o codigo
REM  de saida ao agendador, que mostra a tarefa como "Concluida" ou "Com falha".
REM  O ML (03_ml_clusters.py) NAO e chamado aqui: ele roda uma unica vez, a mao.
REM
REM  Teste manual:  executar_pipeline.bat
REM                 executar_pipeline.bat --sem-download
REM                 executar_pipeline.bat --dry-run
REM ============================================================================

setlocal

REM --- ajuste apenas esta linha se mover o projeto de lugar --------------------
set "PRONAF_RAIZ=D:\Cursos\POS ABI\Projetos Integrados de Analytics"

REM --- se o Python nao estiver no PATH do usuario do agendador, aponte o exe ---
set "PYTHON=python"

if not exist "%PRONAF_RAIZ%" (
    echo [ERRO] Raiz do projeto nao encontrada: "%PRONAF_RAIZ%"
    exit /b 1
)

cd /d "%PRONAF_RAIZ%\scripts" || exit /b 1

echo ============================================================
echo  Pipeline Pronaf RS  -  inicio: %DATE% %TIME%
echo ============================================================

"%PYTHON%" 00_pipeline.py --raiz "%PRONAF_RAIZ%" %*
set "CODIGO=%ERRORLEVEL%"

if not exist "%PRONAF_RAIZ%\data\logs" mkdir "%PRONAF_RAIZ%\data\logs"
> "%PRONAF_RAIZ%\data\logs\ultima_execucao.txt" (
    echo Data......: %DATE% %TIME%
    echo Codigo....: %CODIGO%
    echo Parametros: %*
)

if "%CODIGO%"=="0" echo Resultado: SUCESSO
if "%CODIGO%"=="1" echo Resultado: FALHA em alguma etapa - veja data\logs\
if "%CODIGO%"=="2" echo Resultado: outra execucao ja estava em curso

endlocal & exit /b %CODIGO%
