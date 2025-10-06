-- Script para recrear tablas en nuevo cluster

-- Tablas limpias
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

CREATE EXTERNAL TABLE listening_clean (
    user_id STRING,
    track_id STRING,
    total_playcount INT
)
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/cleaned/listening/';

CREATE EXTERNAL TABLE music_with_stats (
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
    time_signature INT,
    total_plays BIGINT,
    unique_listeners BIGINT,
    avg_plays_per_user DOUBLE,
    popularity_score BIGINT
)
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/cleaned/music_with_stats/';
