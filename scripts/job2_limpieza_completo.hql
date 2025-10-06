-- ============================================================================
-- JOB 2: LIMPIEZA Y NORMALIZACIÓN (CON RECREACIÓN DE TABLAS)
-- ============================================================================
-- Objetivo: Preparar datos limpios para análisis y modelo ALS
-- Input: Parquet files en S3 (del Job 1)
-- Output: music_clean, listening_clean, music_with_stats
-- ============================================================================

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 1000;
SET hive.exec.compress.output = true;
SET parquet.compression = SNAPPY;
SET hive.metastore.warehouse.dir = s3://emr-logs-1758750407/music-data/cleaned/;

-- ============================================================================
-- PASO 0: RECREAR TABLAS RAW (apuntando a datos del Job 1)
-- ============================================================================

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

DROP TABLE IF EXISTS listening_raw;
CREATE EXTERNAL TABLE listening_raw (
    track_id STRING,
    user_id STRING,
    playcount INT
)
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/raw-parquet/listening/';

-- Verificar que las tablas se crearon
SELECT 'Tables created successfully' AS status;
SELECT 'music_raw count' AS metric, COUNT(*) AS value FROM music_raw;
SELECT 'listening_raw count' AS metric, COUNT(*) AS value FROM listening_raw;


-- ============================================================================
-- PASO 1: LIMPIEZA DE TABLA MUSIC
-- ============================================================================

DROP TABLE IF EXISTS music_clean;

CREATE TABLE music_clean
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/cleaned/music/'
AS
SELECT 
    -- Identificadores
    track_id,
    
    -- Información básica (limpia)
    TRIM(title) AS title,
    LOWER(TRIM(artist)) AS artist,
    
    -- URLs Spotify
    spotify_preview_url,
    spotify_id,
    
    -- Tags y género (normalizado)
    tags,
    CASE 
        WHEN genre IS NULL THEN 'unknown'
        WHEN TRIM(genre) = '' THEN 'unknown'
        ELSE LOWER(TRIM(genre))
    END AS genre,
    
    -- Año (0 si es NULL)
    COALESCE(year, 0) AS year,
    
    -- Audio Features
    duration_ms,
    danceability,
    energy,
    key_signature,
    loudness,
    mode,
    speechiness,
    acousticness,
    instrumentalness,
    liveness,
    valence,
    tempo,
    time_signature

FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY track_id) AS rn
    FROM music_raw
    WHERE track_id IS NOT NULL
) ranked
WHERE rn = 1;


-- ============================================================================
-- PASO 2: LIMPIEZA DE TABLA LISTENING
-- ============================================================================

DROP TABLE IF EXISTS listening_clean;

CREATE TABLE listening_clean
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/cleaned/listening/'
AS
SELECT 
    user_id,
    track_id,
    SUM(playcount) AS total_playcount
FROM listening_raw
WHERE track_id IS NOT NULL
  AND user_id IS NOT NULL
  AND playcount IS NOT NULL
  AND playcount > 0
GROUP BY user_id, track_id;


-- ============================================================================
-- PASO 3: TABLA ENRIQUECIDA CON ESTADÍSTICAS
-- ============================================================================

DROP TABLE IF EXISTS music_with_stats;

CREATE TABLE music_with_stats
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/cleaned/music_with_stats/'
AS
SELECT 
    m.*,
    
    -- Métricas de popularidad
    COALESCE(l.total_plays, 0) AS total_plays,
    COALESCE(l.unique_listeners, 0) AS unique_listeners,
    COALESCE(l.avg_plays_per_user, 0.0) AS avg_plays_per_user,
    
    -- Score de popularidad
    COALESCE(l.total_plays, 0) * COALESCE(l.unique_listeners, 0) AS popularity_score
    
FROM music_clean m
LEFT JOIN (
    SELECT 
        track_id,
        SUM(total_playcount) AS total_plays,
        COUNT(DISTINCT user_id) AS unique_listeners,
        AVG(total_playcount) AS avg_plays_per_user
    FROM listening_clean
    GROUP BY track_id
) l ON m.track_id = l.track_id;


-- ============================================================================
-- VERIFICACIONES FINALES
-- ============================================================================

SELECT 'STATISTICS' AS section;

SELECT 'Music raw count' AS metric, COUNT(*) AS value FROM music_raw
UNION ALL
SELECT 'Music clean count', COUNT(*) FROM music_clean
UNION ALL
SELECT 'Music duplicates removed', 
       (SELECT COUNT(*) FROM music_raw) - (SELECT COUNT(*) FROM music_clean)
UNION ALL
SELECT 'Listening raw count', COUNT(*) FROM listening_raw
UNION ALL
SELECT 'Listening clean count', COUNT(*) FROM listening_clean
UNION ALL
SELECT 'Listening invalid removed', 
       (SELECT COUNT(*) FROM listening_raw) - (SELECT COUNT(*) FROM listening_clean)
UNION ALL
SELECT 'Music with stats count', COUNT(*) FROM music_with_stats;


SELECT 'TOP 10 GENRES' AS info;
SELECT genre, COUNT(*) AS count
FROM music_clean
GROUP BY genre
ORDER BY count DESC
LIMIT 10;


SELECT 'TOP 10 ARTISTS' AS info;
SELECT artist, COUNT(*) AS count
FROM music_clean
GROUP BY artist
ORDER BY count DESC
LIMIT 10;


SELECT 'DISTRIBUTION BY DECADE' AS info;
SELECT 
    CASE 
        WHEN year = 0 THEN 'Unknown'
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
FROM music_clean
GROUP BY 
    CASE 
        WHEN year = 0 THEN 'Unknown'
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


SELECT 'SAMPLE music_clean' AS info;
SELECT * FROM music_clean LIMIT 5;

SELECT 'SAMPLE listening_clean' AS info;
SELECT * FROM listening_clean LIMIT 5;

SELECT 'SAMPLE music_with_stats' AS info;
SELECT * FROM music_with_stats LIMIT 5;


-- ============================================================================
-- FIN DEL JOB 2
-- ============================================================================
