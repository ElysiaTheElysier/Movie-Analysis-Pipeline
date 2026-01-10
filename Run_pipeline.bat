@echo off
chcp 65001 >NUL
set PYTHONIOENCODING=utf-8


cd /d "%~dp0"

echo ============================================== >> run_log.txt
echo Date: %date% %time% >> run_log.txt
echo ============================================== >> run_log.txt

echo [1/3] Running Genre ETL...
python Script/get_genre.py >> run_log.txt 2>&1

echo.
echo [2/3] Running Auto ETL (Incremental Load)...
python Script/auto_etl.py >> run_log.txt 2>&1

echo.
echo [3/3] Running Financial Enrichment (Multi-thread)...
python Script/financial.py >> run_log.txt 2>&1

echo.
echo ============================================== >> run_log.txt
echo PIPELINE COMPLETED
echo ==============================================
pause