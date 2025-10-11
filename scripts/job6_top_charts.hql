-- ============================================================================
-- JOB 6: TOP CHARTS MENSUALES (HIVE)
-- ============================================================================
-- Objetivo: Generar rankings de canciones más escuchadas
-- Input: music_clean, listening_clean
-- Output: Top 100 global y por género
-- ============================================================================

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- ============================================================================
-- PASO 0: RECREAR TABLAS CLEANED (apuntan a datos existentes en S3)
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
SELECT 'music_clean count' AS metric, COUNT(*) AS value FROM music_clean;
SELECT 'listening_clean count' AS metric, COUNT(*) AS value FROM listening_clean;

-- ============================================================================
-- ANÁLISIS 1: TOP 100 GLOBAL
-- ============================================================================

DROP TABLE IF EXISTS top_100_global;

CREATE TABLE top_100_global
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/charts/top_100_global/'
AS
SELECT
    m.track_id,
    m.title,
    m.artist,
    m.genre,
    m.year,
    SUM(l.total_playcount) AS total_plays,
    COUNT(DISTINCT l.user_id) AS unique_listeners,
    ROUND(AVG(l.total_playcount), 2) AS avg_plays_per_user,
    ROUND(m.danceability, 3) AS danceability,
    ROUND(m.energy, 3) AS energy,
    ROUND(m.valence, 3) AS valence
FROM music_clean m
JOIN listening_clean l ON m.track_id = l.track_id
GROUP BY
    m.track_id, m.title, m.artist, m.genre, m.year,
    m.danceability, m.energy, m.valence
ORDER BY total_plays DESC
LIMIT 100;

SELECT 'TOP 10 GLOBAL SONGS' AS info;
SELECT title, artist, total_plays, unique_listeners
FROM top_100_global
LIMIT 10;


-- ============================================================================
-- ANÁLISIS 2: TOP 50 POR GÉNERO
-- ============================================================================

DROP TABLE IF EXISTS top_50_by_genre;

CREATE TABLE top_50_by_genre
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/charts/top_50_by_genre/'
AS
SELECT
    genre,
    track_id,
    title,
    artist,
    total_plays,
    unique_listeners,
    genre_rank
FROM (
    SELECT
        m.genre,
        m.track_id,
        m.title,
        m.artist,
        SUM(l.total_playcount) AS total_plays,
        COUNT(DISTINCT l.user_id) AS unique_listeners,
        ROW_NUMBER() OVER (PARTITION BY m.genre ORDER BY SUM(l.total_playcount) DESC) AS genre_rank
    FROM music_clean m
    JOIN listening_clean l ON m.track_id = l.track_id
    WHERE m.genre != 'unknown'
    GROUP BY m.genre, m.track_id, m.title, m.artist
) ranked
WHERE genre_rank <= 50
ORDER BY genre, genre_rank;

SELECT 'TOP 5 PER GENRE SAMPLE' AS info;
SELECT genre, title, artist, total_plays, genre_rank
FROM top_50_by_genre
WHERE genre_rank <= 5
ORDER BY genre, genre_rank
LIMIT 20;


-- ============================================================================
-- ANÁLISIS 3: TOP 50 ARTISTAS
-- ============================================================================

DROP TABLE IF EXISTS top_50_artists;

CREATE TABLE top_50_artists
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/charts/top_50_artists/'
AS
SELECT
    m.artist,
    MAX(m.genre) AS primary_genre,
    COUNT(DISTINCT m.track_id) AS num_songs,
    SUM(l.total_playcount) AS total_plays,
    COUNT(DISTINCT l.user_id) AS unique_listeners,
    ROUND(AVG(l.total_playcount), 2) AS avg_plays_per_song
FROM music_clean m
JOIN listening_clean l ON m.track_id = l.track_id
GROUP BY m.artist
ORDER BY total_plays DESC
LIMIT 50;

SELECT 'TOP 10 ARTISTS' AS info;
SELECT artist, num_songs, total_plays, unique_listeners
FROM top_50_artists
LIMIT 10;


-- ============================================================================
-- RESUMEN FINAL
-- ============================================================================

SELECT '====== JOB 6 COMPLETED ======' AS status;

SELECT 'Top charts created' AS metric, '3 tables' AS value
UNION ALL
SELECT 'Top 100 global', CAST(COUNT(*) AS STRING) FROM top_100_global
UNION ALL
SELECT 'Top 50 by genre', CAST(COUNT(*) AS STRING) FROM top_50_by_genre
UNION ALL
SELECT 'Top 50 artists', CAST(COUNT(*) AS STRING) FROM top_50_artists;

-- ============================================================================
-- FIN DEL JOB 6
-- ============================================================================
