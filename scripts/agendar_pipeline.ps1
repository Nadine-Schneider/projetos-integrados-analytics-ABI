<#
.SYNOPSIS
    Agenda a carga automatizada do projeto Pronaf RS (download incremental + ETL) no
    Agendador de Tarefas do Windows.

.DESCRIPTION
    A tarefa chama scripts\executar_pipeline.bat, que roda o 00_pipeline.py.
    O ML (03_ml_clusters.py) NAO faz parte da tarefa: ele e executado uma unica vez, a mao.

    Se o computador estiver desligado no horario, a tarefa roda assim que ele for ligado.
    Execute este script em um PowerShell **como Administrador**, dentro da pasta scripts\.

.EXAMPLE
    .\agendar_pipeline.ps1                                    # a cada 2 semanas, domingo as 02:00 (padrao)
    .\agendar_pipeline.ps1 -Frequencia Quinzenal -DiaSemana Domingo -Hora "02:00"
    .\agendar_pipeline.ps1 -Frequencia Diaria  -Hora "02:00"
    .\agendar_pipeline.ps1 -Frequencia Semanal -DiaSemana Domingo -Hora "02:00"
    .\agendar_pipeline.ps1 -Frequencia Mensal  -DiaMes 5 -Hora "03:00"
    .\agendar_pipeline.ps1 -Testar     # dispara agora
    .\agendar_pipeline.ps1 -Status     # ultima execucao, resultado e proximo disparo
    .\agendar_pipeline.ps1 -Remover    # apaga a tarefa
#>

param(
    [string]$Raiz = "D:\Cursos\POS ABI\Projetos Integrados de Analytics",
    [ValidateSet("Quinzenal", "Diaria", "Semanal", "Mensal")]
    [string]$Frequencia = "Quinzenal",
    [ValidatePattern("^([01]\d|2[0-3]):[0-5]\d$")]
    [string]$Hora = "02:00",
    [ValidateSet("Domingo", "Segunda", "Terca", "Quarta", "Quinta", "Sexta", "Sabado")]
    [string]$DiaSemana = "Domingo",
    [ValidateRange(1, 28)]
    [int]$DiaMes = 5,
    [string]$Nome = "Pronaf RS - Carga automatizada",
    [switch]$Testar,
    [switch]$Status,
    [switch]$Remover
)

$ErrorActionPreference = "Stop"
$Bat = Join-Path $Raiz "scripts\executar_pipeline.bat"

# ---------------------------------------------------------------- remover ----
if ($Remover) {
    Unregister-ScheduledTask -TaskName $Nome -Confirm:$false
    Write-Host "Tarefa '$Nome' removida." -ForegroundColor Yellow
    return
}

# ----------------------------------------------------------------- status ----
if ($Status) {
    $info = Get-ScheduledTaskInfo -TaskName $Nome
    $res = if ($info.LastTaskResult -eq 0) { "0 (sucesso)" }
           elseif ($info.LastTaskResult -eq 267011) { "ainda nao executada" }
           else { "$($info.LastTaskResult) (falha - veja data\logs)" }
    Write-Host "Tarefa            : $Nome"
    Write-Host "Ultima execucao   : $($info.LastRunTime)"
    Write-Host "Resultado         : $res"
    Write-Host "Proxima execucao  : $($info.NextRunTime)"
    $ult = Join-Path $Raiz "data\logs\ultima_execucao.txt"
    if (Test-Path $ult) { Write-Host "`n--- ultima_execucao.txt ---" -ForegroundColor Cyan; Get-Content $ult }
    return
}

# ----------------------------------------------------------------- testar ----
if ($Testar) {
    Start-ScheduledTask -TaskName $Nome
    Write-Host "Tarefa disparada. Acompanhe o log mais recente em $(Join-Path $Raiz 'data\logs')" -ForegroundColor Cyan
    return
}

# --------------------------------------------------------------- registrar ---
if (-not (Test-Path $Bat)) { throw "Nao encontrei $Bat. Confira o parametro -Raiz." }

$diasEn = @{ Domingo = "Sunday"; Segunda = "Monday"; Terca = "Tuesday"; Quarta = "Wednesday";
             Quinta = "Thursday"; Sexta = "Friday"; Sabado = "Saturday" }
$quando = [datetime]::ParseExact($Hora, "HH:mm", $null)

switch ($Frequencia) {
    "Quinzenal" { # semanal com intervalo de 2 semanas: roda a cada 14 dias, no dia da semana escolhido
                $gatilho = New-ScheduledTaskTrigger -Weekly -WeeksInterval 2 -DaysOfWeek $diasEn[$DiaSemana] -At $quando
                $texto = "a cada 2 semanas, $DiaSemana, as $Hora" }
    "Diaria"  { $gatilho = New-ScheduledTaskTrigger -Daily -At $quando
                $texto = "todos os dias, as $Hora" }
    "Semanal" { $gatilho = New-ScheduledTaskTrigger -Weekly -DaysOfWeek $diasEn[$DiaSemana] -At $quando
                $texto = "toda(o) $DiaSemana, as $Hora" }
    "Mensal"  { # New-ScheduledTaskTrigger nao tem opcao mensal: usa o schtasks para criar
                schtasks /Create /TN "$Nome" /TR "`"$Bat`"" /SC MONTHLY /D $DiaMes /ST $Hora /RL HIGHEST /F | Out-Null
                $texto = "todo dia $DiaMes do mes, as $Hora" }
}

$config = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 6) `
    -RestartCount 2 -RestartInterval (New-TimeSpan -Minutes 30) -MultipleInstances IgnoreNew

if ($Frequencia -eq "Mensal") {
    Set-ScheduledTask -TaskName $Nome -Settings $config | Out-Null
} else {
    $acao = New-ScheduledTaskAction -Execute $Bat -WorkingDirectory (Split-Path $Bat)
    $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest
    Register-ScheduledTask -TaskName $Nome -Action $acao -Trigger $gatilho -Settings $config `
        -Principal $principal -Description "Pronaf RS: download dos ultimos 3 meses do SICOR + ETL" -Force | Out-Null
}

Write-Host "Tarefa registrada" -ForegroundColor Green
Write-Host "  Nome      : $Nome"
Write-Host "  Comando   : $Bat"
Write-Host "  Frequencia: $texto"
Write-Host "  Se o PC estiver desligado no horario, roda ao ser ligado; 2 retentativas; limite de 6 h."
Write-Host "`nProximos passos:" -ForegroundColor Green
Write-Host "  .\agendar_pipeline.ps1 -Testar   # dispara agora e confere ponta a ponta"
Write-Host "  .\agendar_pipeline.ps1 -Status   # resultado da ultima execucao e proximo disparo"
