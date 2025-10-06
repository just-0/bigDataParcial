-- ============================================================================
-- JOB 3: ANÁLISIS EXPLORATORIO
-- ============================================================================
-- Objetivo: Generar insights sobre el dataset limpio
-- Input: music_clean, listening_clean, music_with_stats
-- Output: Reportes y tablas agregadas para visualización
-- ============================================================================

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- ============================================================================
-- PASO 0: VERIFICAR TABLAS LIMPIAS
-- ============================================================================

SELECT 'INITIAL VERIFICATION' AS section;
SELECT 'music_clean' AS table_name, COUNT(*) AS records FROM music_clean
UNION ALL
SELECT 'listening_clean', COUNT(*) FROM listening_clean
UNION ALL
SELECT 'music_with_stats', COUNT(*) FROM music_with_stats;


-- ============================================================================
-- ANÁLISIS 1: ESTADÍSTICAS DE AUDIO FEATURES
-- ============================================================================

DROP TABLE IF EXISTS audio_features_stats;

CREATE TABLE audio_features_stats
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/analysis/audio_features_stats/'
AS
SELECT
    -- Estadísticas globales
    'Global' AS segment,
    COUNT(*) AS total_tracks,
    
    -- Duración
    ROUND(AVG(duration_ms)/1000, 2) AS avg_duration_seconds,
    ROUND(MIN(duration_ms)/1000, 2) AS min_duration_seconds,
    ROUND(MAX(duration_ms)/1000, 2) AS max_duration_seconds,
    
    -- Danceability
    ROUND(AVG(danceability), 3) AS avg_danceability,
    ROUND(STDDEV_POP(danceability), 3) AS std_danceability,
    
    -- Energy
    ROUND(AVG(energy), 3) AS avg_energy,
    ROUND(STDDEV_POP(energy), 3) AS std_energy,
    
    -- Valence (positividad)
    ROUND(AVG(valence), 3) AS avg_valence,
    ROUND(STDDEV_POP(valence), 3) AS std_valence,
    
    -- Tempo
    ROUND(AVG(tempo), 2) AS avg_tempo_bpm,
    ROUND(STDDEV_POP(tempo), 2) AS std_tempo_bpm,
    
    -- Acousticness
    ROUND(AVG(acousticness), 3) AS avg_acousticness,
    
    -- Instrumentalness
    ROUND(AVG(instrumentalness), 3) AS avg_instrumentalness,
    
    -- Loudness
    ROUND(AVG(loudness), 2) AS avg_loudness_db,
    ROUND(MIN(loudness), 2) AS min_loudness_db,
    ROUND(MAX(loudness), 2) AS max_loudness_db

FROM music_clean
WHERE duration_ms > 0 AND duration_ms < 600000; -- Filtrar outliers (< 10 min)

-- Ver resultados
SELECT 'AUDIO FEATURES STATISTICS' AS info;
SELECT * FROM audio_features_stats;


-- ============================================================================
-- ANÁLISIS 2: EVOLUCIÓN POR DÉCADA
-- ============================================================================

DROP TABLE IF EXISTS decade_evolution;

CREATE TABLE decade_evolution
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/analysis/decade_evolution/'
AS
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
    
    COUNT(*) AS num_songs,
    COUNT(DISTINCT artist) AS unique_artists,
    
    -- Audio features promedio por década
    ROUND(AVG(duration_ms)/1000, 2) AS avg_duration_sec,
    ROUND(AVG(tempo), 2) AS avg_tempo,
    ROUND(AVG(energy), 3) AS avg_energy,
    ROUND(AVG(danceability), 3) AS avg_danceability,
    ROUND(AVG(valence), 3) AS avg_valence,
    ROUND(AVG(acousticness), 3) AS avg_acousticness,
    ROUND(AVG(loudness), 2) AS avg_loudness

FROM music_clean
WHERE duration_ms > 0 AND duration_ms < 600000
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

-- Ver resultados
SELECT 'DECADE EVOLUTION' AS info;
SELECT * FROM decade_evolution ORDER BY decade;


-- ============================================================================
-- ANÁLISIS 3: GÉNEROS MÁS POPULARES
-- ============================================================================

DROP TABLE IF EXISTS genre_popularity;

CREATE TABLE genre_popularity
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/analysis/genre_popularity/'
AS
SELECT 
    m.genre,
    COUNT(DISTINCT m.track_id) AS num_songs,
    COUNT(DISTINCT m.artist) AS num_artists,
    
    -- Métricas de listening
    SUM(COALESCE(l.total_playcount, 0)) AS total_plays,
    COUNT(DISTINCT l.user_id) AS unique_listeners,
    ROUND(AVG(COALESCE(l.total_playcount, 0)), 2) AS avg_plays_per_song,
    
    -- Audio features del género
    ROUND(AVG(m.energy), 3) AS avg_energy,
    ROUND(AVG(m.danceability), 3) AS avg_danceability,
    ROUND(AVG(m.valence), 3) AS avg_valence,
    ROUND(AVG(m.tempo), 2) AS avg_tempo

FROM music_clean m
LEFT JOIN listening_clean l ON m.track_id = l.track_id
WHERE m.genre != 'unknown'
GROUP BY m.genre
HAVING COUNT(DISTINCT m.track_id) >= 10  -- Filtrar géneros con al menos 10 canciones
ORDER BY total_plays DESC;

-- Top 20 géneros
SELECT 'TOP 20 GENRES BY POPULARITY' AS info;
SELECT 
    genre,
    num_songs,
    num_artists,
    total_plays,
    unique_listeners,
    avg_plays_per_song,
    avg_energy,
    avg_danceability
FROM genre_popularity
ORDER BY total_plays DESC
LIMIT 20;


-- ============================================================================
-- ANÁLISIS 4: ARTISTAS MÁS ESCUCHADOS
-- ============================================================================

DROP TABLE IF EXISTS artist_popularity;

CREATE TABLE artist_popularity
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/analysis/artist_popularity/'
AS
SELECT 
    m.artist,
    COUNT(DISTINCT m.track_id) AS num_songs,
    
    -- Métricas de listening
    SUM(COALESCE(l.total_playcount, 0)) AS total_plays,
    COUNT(DISTINCT l.user_id) AS unique_listeners,
    ROUND(AVG(COALESCE(l.total_playcount, 0)), 2) AS avg_plays_per_song,
    
    -- Géneros del artista (toma el más común)
    MAX(m.genre) AS primary_genre,
    
    -- Años de actividad
    MIN(CASE WHEN m.year > 0 THEN m.year END) AS first_year,
    MAX(CASE WHEN m.year > 0 THEN m.year END) AS last_year

FROM music_clean m
LEFT JOIN listening_clean l ON m.track_id = l.track_id
GROUP BY m.artist
HAVING COUNT(DISTINCT m.track_id) >= 3  -- Al menos 3 canciones
ORDER BY total_plays DESC;

-- Top 30 artistas
SELECT 'TOP 30 ARTISTS BY PLAYS' AS info;
SELECT 
    artist,
    num_songs,
    total_plays,
    unique_listeners,
    avg_plays_per_song,
    primary_genre,
    CONCAT(first_year, '-', last_year) AS active_years
FROM artist_popularity
ORDER BY total_plays DESC
LIMIT 30;


-- ============================================================================
-- ANÁLISIS 5: DISTRIBUCIÓN DE USUARIOS
-- ============================================================================

DROP TABLE IF EXISTS user_behavior;

CREATE TABLE user_behavior
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/analysis/user_behavior/'
AS
SELECT
    user_id,
    COUNT(DISTINCT track_id) AS unique_songs_played,
    SUM(total_playcount) AS total_plays,
    ROUND(AVG(total_playcount), 2) AS avg_plays_per_song,
    MAX(total_playcount) AS max_plays_single_song
FROM listening_clean
GROUP BY user_id;

-- Estadísticas de usuarios
SELECT 'USER BEHAVIOR STATS' AS info;
SELECT
    'Total users' AS metric,
    COUNT(*) AS value
FROM user_behavior
UNION ALL
SELECT 
    'Avg songs per user',
    ROUND(AVG(unique_songs_played), 2)
FROM user_behavior
UNION ALL
SELECT 
    'Avg total plays per user',
    ROUND(AVG(total_plays), 2)
FROM user_behavior
UNION ALL
SELECT 
    'Power users (>100 songs)',
    COUNT(*)
FROM user_behavior
WHERE unique_songs_played > 100;

-- Segmentación de usuarios
SELECT 'USER SEGMENTATION' AS info;
SELECT
    CASE
        WHEN unique_songs_played < 5 THEN '1. Casual (1-4 songs)'
        WHEN unique_songs_played < 20 THEN '2. Regular (5-19 songs)'
        WHEN unique_songs_played < 50 THEN '3. Active (20-49 songs)'
        WHEN unique_songs_played < 100 THEN '4. Heavy (50-99 songs)'
        ELSE '5. Power User (100+ songs)'
    END AS user_segment,
    COUNT(*) AS num_users,
    ROUND(AVG(total_plays), 2) AS avg_total_plays,
    ROUND(AVG(avg_plays_per_song), 2) AS avg_plays_per_song
FROM user_behavior
GROUP BY
    CASE
        WHEN unique_songs_played < 5 THEN '1. Casual (1-4 songs)'
        WHEN unique_songs_played < 20 THEN '2. Regular (5-19 songs)'
        WHEN unique_songs_played < 50 THEN '3. Active (20-49 songs)'
        WHEN unique_songs_played < 100 THEN '4. Heavy (50-99 songs)'
        ELSE '5. Power User (100+ songs)'
    END
ORDER BY user_segment;


-- ============================================================================
-- ANÁLISIS 6: CANCIONES MÁS POPULARES
-- ============================================================================

SELECT 'TOP 50 MOST PLAYED SONGS' AS info;
SELECT 
    m.title,
    m.artist,
    m.genre,
    m.year,
    ms.total_plays,
    ms.unique_listeners,
    ROUND(ms.avg_plays_per_user, 2) AS avg_plays_per_user,
    ROUND(m.energy, 2) AS energy,
    ROUND(m.danceability, 2) AS danceability
FROM music_with_stats ms
JOIN music_clean m ON ms.track_id = m.track_id
WHERE ms.total_plays > 0
ORDER BY ms.total_plays DESC
LIMIT 50;


-- ============================================================================
-- ANÁLISIS 7: CORRELACIONES ENTRE AUDIO FEATURES Y POPULARIDAD
-- ============================================================================

SELECT 'CORRELATION: AUDIO FEATURES vs POPULARITY' AS info;

-- Canciones muy populares vs poco populares
SELECT 
    CASE 
        WHEN total_plays >= 1000 THEN 'High Popularity (1000+ plays)'
        WHEN total_plays >= 100 THEN 'Medium Popularity (100-999 plays)'
        WHEN total_plays >= 10 THEN 'Low Popularity (10-99 plays)'
        ELSE 'Very Low Popularity (<10 plays)'
    END AS popularity_tier,
    
    COUNT(*) AS num_songs,
    ROUND(AVG(m.energy), 3) AS avg_energy,
    ROUND(AVG(m.danceability), 3) AS avg_danceability,
    ROUND(AVG(m.valence), 3) AS avg_valence,
    ROUND(AVG(m.tempo), 2) AS avg_tempo,
    ROUND(AVG(m.acousticness), 3) AS avg_acousticness,
    ROUND(AVG(m.duration_ms)/1000, 2) AS avg_duration_sec

FROM music_with_stats ms
JOIN music_clean m ON ms.track_id = m.track_id
GROUP BY 
    CASE 
        WHEN total_plays >= 1000 THEN 'High Popularity (1000+ plays)'
        WHEN total_plays >= 100 THEN 'Medium Popularity (100-999 plays)'
        WHEN total_plays >= 10 THEN 'Low Popularity (10-99 plays)'
        ELSE 'Very Low Popularity (<10 plays)'
    END
ORDER BY 
    CASE 
        WHEN popularity_tier = 'High Popularity (1000+ plays)' THEN 1
        WHEN popularity_tier = 'Medium Popularity (100-999 plays)' THEN 2
        WHEN popularity_tier = 'Low Popularity (10-99 plays)' THEN 3
        ELSE 4
    END;


-- ============================================================================
-- RESUMEN FINAL
-- ============================================================================

SELECT '====== JOB 3 COMPLETED ======' AS status;

SELECT 'Tables created' AS metric, '7' AS value
UNION ALL
SELECT 'Total songs analyzed', CAST(COUNT(*) AS STRING) FROM music_clean
UNION ALL
SELECT 'Total listening events', CAST(COUNT(*) AS STRING) FROM listening_clean
UNION ALL
SELECT 'Unique users', CAST(COUNT(DISTINCT user_id) AS STRING) FROM listening_clean
UNION ALL
SELECT 'Genres identified', CAST(COUNT(DISTINCT genre) AS STRING) FROM music_clean WHERE genre != 'unknown'
UNION ALL
SELECT 'Artists identified', CAST(COUNT(DISTINCT artist) AS STRING) FROM music_clean;

-- ============================================================================
-- FIN DEL JOB 3
-- ============================================================================
