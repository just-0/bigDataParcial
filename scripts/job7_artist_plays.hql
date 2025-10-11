-- ============================================================================
-- JOB 7: PREPARAR DATOS PARA MAPREDUCE - ARTIST PLAYS
-- ============================================================================
-- Paso 1: Crear tabla temporal con datos para MapReduce
-- ============================================================================

-- Paso 0: Recrear tablas cleaned (apuntan a datos existentes en S3)
DROP TABLE IF EXISTS music_clean;
CREATE EXTERNAL TABLE music_clean (
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
LOCATION 's3://emr-logs-1758750407/music-data/cleaned/music/';

DROP TABLE IF EXISTS listening_clean;
CREATE EXTERNAL TABLE listening_clean (
    user_id STRING,
    track_id STRING,
    total_playcount INT
)
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/cleaned/listening/';

SELECT 'Tables recreated for Job 7' AS status;

DROP TABLE IF EXISTS artist_plays_input;

CREATE EXTERNAL TABLE artist_plays_input
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://emr-logs-1758750407/music-data/mapreduce/job7_input/'
AS
SELECT
    l.user_id,
    l.track_id,
    l.total_playcount,
    m.artist
FROM listening_clean l
JOIN music_clean m ON l.track_id = m.track_id;

SELECT 'JOB 7 INPUT PREPARED' AS status;
SELECT 'Total records' AS metric, COUNT(*) AS value FROM artist_plays_input;
SELECT 'Sample data' AS info;
SELECT * FROM artist_plays_input LIMIT 5;
