-- ============================================================================
-- JOBS 7-10: PREPARAR TODOS LOS INPUTS PARA MAPREDUCE
-- ============================================================================
-- Este script crea las 4 tablas input que necesitan los jobs MapReduce
-- Se ejecuta UNA VEZ en el cluster antes de los steps MapReduce
-- ============================================================================

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- ============================================================================
-- PASO 0: RECREAR TABLAS CLEANED UNA SOLA VEZ
-- ============================================================================

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

SELECT 'Tables recreated' AS status;

-- ============================================================================
-- JOB 7 INPUT: Artist plays (user_id, track_id, playcount, artist)
-- ============================================================================

DROP TABLE IF EXISTS artist_plays_input;

CREATE TABLE artist_plays_input
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

-- ============================================================================
-- JOB 8 INPUT: User genres (user_id, genre, playcount)
-- ============================================================================

DROP TABLE IF EXISTS user_genres_input;

CREATE TABLE user_genres_input
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

-- ============================================================================
-- JOB 9 INPUT: Decade stats (year, danceability, energy, tempo, valence)
-- ============================================================================

DROP TABLE IF EXISTS decade_stats_input;

CREATE TABLE decade_stats_input
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

-- ============================================================================
-- JOB 10 INPUT: User activity (user_id, track_id, playcount)
-- ============================================================================

DROP TABLE IF EXISTS user_activity_input;

CREATE TABLE user_activity_input
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://emr-logs-1758750407/music-data/mapreduce/job10_input/'
AS
SELECT
    user_id,
    track_id,
    total_playcount
FROM listening_clean;

SELECT 'JOB 10 INPUT PREPARED' AS status;

-- ============================================================================
-- RESUMEN
-- ============================================================================

SELECT '====== ALL INPUTS PREPARED ======' AS status;
SELECT 'Job 7 input records', CAST(COUNT(*) AS STRING) FROM artist_plays_input
UNION ALL
SELECT 'Job 8 input records', CAST(COUNT(*) AS STRING) FROM user_genres_input
UNION ALL
SELECT 'Job 9 input records', CAST(COUNT(*) AS STRING) FROM decade_stats_input
UNION ALL
SELECT 'Job 10 input records', CAST(COUNT(*) AS STRING) FROM user_activity_input;

-- ============================================================================
-- FIN
-- ============================================================================
