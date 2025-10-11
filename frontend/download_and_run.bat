@echo off
echo ========================================
echo Descargando resultados de S3...
echo ========================================

cd /d "%~dp0"

rem Crear carpetas
mkdir data 2>nul
mkdir data\charts 2>nul
mkdir data\job7_artist_plays 2>nul
mkdir data\job8_user_genres 2>nul
mkdir data\job9_decade_stats 2>nul
mkdir data\job10_user_activity 2>nul
mkdir data\json 2>nul

rem Job 6 - Charts
echo Descargando Job 6 (Charts)...
aws s3 sync s3://emr-logs-1758750407/music-data/charts/top_20_sample/ data/charts/top_20_sample/ --profile dev --region us-east-1

rem Job 7 - Artist Plays
echo Descargando Job 7 (Artist Plays)...
aws s3 sync s3://emr-logs-1758750407/music-data/mapreduce/job7_output/ data/job7_artist_plays/ --profile dev --region us-east-1

rem Job 8 - User Genres
echo Descargando Job 8 (User Genres)...
aws s3 sync s3://emr-logs-1758750407/music-data/mapreduce/job8_output/ data/job8_user_genres/ --profile dev --region us-east-1

rem Job 9 - Decade Stats
echo Descargando Job 9 (Decade Stats)...
aws s3 sync s3://emr-logs-1758750407/music-data/mapreduce/job9_output/ data/job9_decade_stats/ --profile dev --region us-east-1

rem Job 10 - User Activity
echo Descargando Job 10 (User Activity)...
aws s3 sync s3://emr-logs-1758750407/music-data/mapreduce/job10_output/ data/job10_user_activity/ --profile dev --region us-east-1

echo.
echo ========================================
echo Convirtiendo datos a JSON...
echo ========================================

python convert_parquet_to_json.py

echo.
echo ========================================
echo Listo! Abriendo dashboard...
echo ========================================

start index.html

pause
