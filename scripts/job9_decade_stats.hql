-- ============================================================================
-- JOB 9: PREPARAR DATOS PARA MAPREDUCE - DECADE STATS
-- ============================================================================

-- Paso 0: Recrear tabla music_clean (apunta a datos existentes en S3)
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

SELECT 'Tables recreated for Job 9' AS status;

DROP TABLE IF EXISTS decade_stats_input;

CREATE EXTERNAL TABLE decade_stats_input
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://emr-logs-1758750407/music-data/mapreduce/job9_input/'
AS
SELECT
    year,
    danceability,
    energy,
    tempo,
    valence
FROM music_clean;

SELECT 'JOB 9 INPUT PREPARED' AS status;
SELECT 'Total records' AS metric, COUNT(*) AS value FROM decade_stats_input;
