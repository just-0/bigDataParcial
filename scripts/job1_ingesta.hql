-- ============================================
-- JOB 1: INGESTA - CSV a Parquet (CORREGIDO v2)
-- ============================================
-- Fix: Separar archivos CSV en carpetas distintas
-- ============================================

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapreduce.map.output.compress=true;

-- ============================================
-- PASO 1A: Leer Music Info CSV
-- ============================================
DROP TABLE IF EXISTS music_csv_temp;

CREATE EXTERNAL TABLE music_csv_temp (
    track_id STRING,
    name STRING,
    artist STRING,
    spotify_preview_url STRING,
    spotify_id STRING,
    tags STRING,
    genre STRING,
    year STRING,
    duration_ms STRING,
    danceability STRING,
    energy STRING,
    key_val STRING,
    loudness STRING,
    mode STRING,
    speechiness STRING,
    acousticness STRING,
    instrumentalness STRING,
    liveness STRING,
    valence STRING,
    tempo STRING,
    time_signature STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://emr-logs-1758750407/music-data/raw-csv/music/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

-- Verificar lectura
SELECT 'Music CSV rows' AS metric, COUNT(*) AS value FROM music_csv_temp;

-- ============================================
-- PASO 1B: Crear tabla Parquet vacía para Music
-- ============================================
DROP TABLE IF EXISTS music_raw;

CREATE EXTERNAL TABLE music_raw (
    track_id STRING,
    title STRING,
    artist STRING,
    spotify_preview_url STRING,
    spotify_id STRING,
    tags STRING,
    genre STRING,
    year INT,
    duration_ms BIGINT,
    danceability DOUBLE,
    energy DOUBLE,
    key_signature INT,
    loudness DOUBLE,
    mode INT,
    speechiness DOUBLE,
    acousticness DOUBLE,
    instrumentalness DOUBLE,
    liveness DOUBLE,
    valence DOUBLE,
    tempo DOUBLE,
    time_signature INT
)
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/raw-parquet/music/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ============================================
-- PASO 1C: Insertar datos con conversión
-- ============================================
INSERT OVERWRITE TABLE music_raw
SELECT 
    track_id,
    name AS title,
    artist,
    spotify_preview_url,
    spotify_id,
    tags,
    genre,
    CAST(year AS INT),
    CAST(duration_ms AS BIGINT),
    CAST(danceability AS DOUBLE),
    CAST(energy AS DOUBLE),
    CAST(key_val AS INT),
    CAST(loudness AS DOUBLE),
    CAST(mode AS INT),
    CAST(speechiness AS DOUBLE),
    CAST(acousticness AS DOUBLE),
    CAST(instrumentalness AS DOUBLE),
    CAST(liveness AS DOUBLE),
    CAST(valence AS DOUBLE),
    CAST(tempo AS DOUBLE),
    CAST(time_signature AS INT)
FROM music_csv_temp
WHERE track_id IS NOT NULL 
  AND track_id != 'track_id';

-- Verificar
SELECT 'Music Parquet rows' AS metric, COUNT(*) AS value FROM music_raw;
SELECT 'Music Parquet sample' AS info, track_id, title, artist, year FROM music_raw LIMIT 5;

-- ============================================
-- PASO 2A: Leer Listening History CSV
-- ============================================
DROP TABLE IF EXISTS listening_csv_temp;

CREATE EXTERNAL TABLE listening_csv_temp (
    track_id STRING,
    user_id STRING,
    playcount STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://emr-logs-1758750407/music-data/raw-csv/listening/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

SELECT 'Listening CSV rows' AS metric, COUNT(*) AS value FROM listening_csv_temp;

-- ============================================
-- PASO 2B: Crear tabla Parquet vacía para Listening
-- ============================================
DROP TABLE IF EXISTS listening_raw;

CREATE EXTERNAL TABLE listening_raw (
    user_id STRING,
    track_id STRING,
    playcount INT
)
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/raw-parquet/listening/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ============================================
-- PASO 2C: Insertar datos de listening
-- ============================================
INSERT OVERWRITE TABLE listening_raw
SELECT 
    user_id,
    track_id,
    CAST(playcount AS INT)
FROM listening_csv_temp
WHERE track_id IS NOT NULL 
  AND track_id != 'track_id'
  AND user_id IS NOT NULL
  AND user_id != 'user_id'
  AND playcount IS NOT NULL;

SELECT 'Listening Parquet rows' AS metric, COUNT(*) AS value FROM listening_raw;
SELECT 'Listening Parquet sample' AS info, user_id, track_id, playcount FROM listening_raw LIMIT 5;

-- ============================================
-- PASO 3: Estadísticas finales
-- ============================================
SELECT 
    'JOB 1 COMPLETADO' AS status,
    (SELECT COUNT(*) FROM music_raw) AS total_songs,
    (SELECT COUNT(*) FROM listening_raw) AS total_interactions,
    (SELECT COUNT(DISTINCT user_id) FROM listening_raw) AS unique_users,
    (SELECT COUNT(DISTINCT track_id) FROM listening_raw) AS unique_tracks_played;

-- Top 10 géneros
SELECT 'TOP 10 GENRES' AS info;
SELECT 
    genre,
    COUNT(*) AS num_songs
FROM music_raw
WHERE genre IS NOT NULL AND genre != ''
GROUP BY genre
ORDER BY num_songs DESC
LIMIT 10;

-- Top 10 artistas
SELECT 'TOP 10 ARTISTS' AS info;
SELECT 
    artist,
    COUNT(*) AS num_songs
FROM music_raw
WHERE artist IS NOT NULL AND artist != ''
GROUP BY artist
ORDER BY num_songs DESC
LIMIT 10;

-- Distribución por década
SELECT 'DISTRIBUTION BY DECADE' AS info;
SELECT 
    CASE 
        WHEN year IS NULL THEN 'Unknown'
        WHEN year < 1950 THEN 'Pre-1950'
        WHEN year BETWEEN 1950 AND 1959 THEN '1950s'
        WHEN year BETWEEN 1960 AND 1969 THEN '1960s'
        WHEN year BETWEEN 1970 AND 1979 THEN '1970s'
        WHEN year BETWEEN 1980 AND 1989 THEN '1980s'
        WHEN year BETWEEN 1990 AND 1999 THEN '1990s'
        WHEN year BETWEEN 2000 AND 2009 THEN '2000s'
        WHEN year >= 2010 THEN '2010s+'
    END AS decade,
    COUNT(*) AS count
FROM music_raw
GROUP BY 
    CASE 
        WHEN year IS NULL THEN 'Unknown'
        WHEN year < 1950 THEN 'Pre-1950'
        WHEN year BETWEEN 1950 AND 1959 THEN '1950s'
        WHEN year BETWEEN 1960 AND 1969 THEN '1960s'
        WHEN year BETWEEN 1970 AND 1979 THEN '1970s'
        WHEN year BETWEEN 1980 AND 1989 THEN '1980s'
        WHEN year BETWEEN 1990 AND 1999 THEN '1990s'
        WHEN year BETWEEN 2000 AND 2009 THEN '2000s'
        WHEN year >= 2010 THEN '2010s+'
    END
ORDER BY decade;

-- ============================================
-- FIN JOB 1
-- ============================================
