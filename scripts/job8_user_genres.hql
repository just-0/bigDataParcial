-- ============================================================================
-- JOB 8: PREPARAR DATOS PARA MAPREDUCE - USER GENRES
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

SELECT 'Tables recreated for Job 8' AS status;

DROP TABLE IF EXISTS user_genres_input;

CREATE EXTERNAL TABLE user_genres_input
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://emr-logs-1758750407/music-data/mapreduce/job8_input/'
AS
SELECT
    l.user_id,
    m.genre,
    l.total_playcount
FROM listening_clean l
JOIN music_clean m ON l.track_id = m.track_id
WHERE m.genre != 'unknown';

SELECT 'JOB 8 INPUT PREPARED' AS status;
SELECT 'Total records' AS metric, COUNT(*) AS value FROM user_genres_input;
