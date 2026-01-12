@echo off
chcp 65001 >NUL
set PYTHONIOENCODING=utf-8

cd /d "%~dp0"

echo ============================================== >> run_log.txt
echo Date: %date% %time% >> run_log.txt
echo ============================================== >> run_log.txt

echo [1/2] Updating Genre List (Dim_Genres)...
python Script/get_genre.py >> run_log.txt 2>&1

echo.
echo [2/2] Running Main ETL Pipeline (Star Schema)...
:: Chạy file mới của bạn (final_etl.py)
python Script/final_etl.py >> run_log.txt 2>&1

echo.
echo ============================================== >> run_log.txt
echo PIPELINE COMPLETED
echo ==============================================
pause