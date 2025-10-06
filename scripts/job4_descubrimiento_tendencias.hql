-- ============================================================================
-- JOB 4: DESCUBRIMIENTO DE TENDENCIAS
-- ============================================================================
-- Objetivo: Identificar géneros y artistas emergentes por década/época
-- Input: music_clean, listening_clean, music_with_stats
-- Output: Tablas de tendencias para dashboards
-- ============================================================================

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- ============================================================================
-- ANÁLISIS 1: TOP GÉNEROS POR DÉCADA
-- ============================================================================

DROP TABLE IF EXISTS genre_trends_by_decade;

CREATE TABLE genre_trends_by_decade
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/trends/genre_by_decade/'
AS
SELECT
    CASE 
        WHEN m.year = 0 THEN 'Unknown'
        WHEN m.year < 1950 THEN 'Pre-1950'
        WHEN m.year BETWEEN 1950 AND 1959 THEN '1950s'
        WHEN m.year BETWEEN 1960 AND 1969 THEN '1960s'
        WHEN m.year BETWEEN 1970 AND 1979 THEN '1970s'
        WHEN m.year BETWEEN 1980 AND 1989 THEN '1980s'
        WHEN m.year BETWEEN 1990 AND 1999 THEN '1990s'
        WHEN m.year BETWEEN 2000 AND 2009 THEN '2000s'
        WHEN m.year >= 2010 THEN '2010s+'
    END AS decade,
    
    m.genre,
    COUNT(DISTINCT m.track_id) AS num_songs,
    COUNT(DISTINCT m.artist) AS num_artists,
    SUM(COALESCE(l.total_playcount, 0)) AS total_plays,
    COUNT(DISTINCT l.user_id) AS unique_listeners,
    
    -- Ranking dentro de la década
    ROW_NUMBER() OVER (
        PARTITION BY 
            CASE 
                WHEN m.year = 0 THEN 'Unknown'
                WHEN m.year < 1950 THEN 'Pre-1950'
                WHEN m.year BETWEEN 1950 AND 1959 THEN '1950s'
                WHEN m.year BETWEEN 1960 AND 1969 THEN '1960s'
                WHEN m.year BETWEEN 1970 AND 1979 THEN '1970s'
                WHEN m.year BETWEEN 1980 AND 1989 THEN '1980s'
                WHEN m.year BETWEEN 1990 AND 1999 THEN '1990s'
                WHEN m.year BETWEEN 2000 AND 2009 THEN '2000s'
                WHEN m.year >= 2010 THEN '2010s+'
            END
        ORDER BY SUM(COALESCE(l.total_playcount, 0)) DESC
    ) AS popularity_rank

FROM music_clean m
LEFT JOIN listening_clean l ON m.track_id = l.track_id
WHERE m.genre != 'unknown'
GROUP BY 
    CASE 
        WHEN m.year = 0 THEN 'Unknown'
        WHEN m.year < 1950 THEN 'Pre-1950'
        WHEN m.year BETWEEN 1950 AND 1959 THEN '1950s'
        WHEN m.year BETWEEN 1960 AND 1969 THEN '1960s'
        WHEN m.year BETWEEN 1970 AND 1979 THEN '1970s'
        WHEN m.year BETWEEN 1980 AND 1989 THEN '1980s'
        WHEN m.year BETWEEN 1990 AND 1999 THEN '1990s'
        WHEN m.year BETWEEN 2000 AND 2009 THEN '2000s'
        WHEN m.year >= 2010 THEN '2010s+'
    END,
    m.genre
HAVING COUNT(DISTINCT m.track_id) >= 5;

-- Top 10 géneros por década
SELECT 'TOP 10 GENRES PER DECADE' AS info;
SELECT 
    decade,
    genre,
    num_songs,
    num_artists,
    total_plays,
    unique_listeners,
    popularity_rank
FROM genre_trends_by_decade
WHERE popularity_rank <= 10
ORDER BY decade, popularity_rank;


-- ============================================================================
-- ANÁLISIS 2: EVOLUCIÓN DE GÉNEROS ESPECÍFICOS
-- ============================================================================

DROP TABLE IF EXISTS genre_evolution;

CREATE TABLE genre_evolution
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/trends/genre_evolution/'
AS
SELECT
    m.genre,
    m.year,
    COUNT(DISTINCT m.track_id) AS num_songs,
    COUNT(DISTINCT m.artist) AS num_artists,
    SUM(COALESCE(l.total_playcount, 0)) AS total_plays,
    
    -- Audio features promedio por año
    ROUND(AVG(m.energy), 3) AS avg_energy,
    ROUND(AVG(m.danceability), 3) AS avg_danceability,
    ROUND(AVG(m.tempo), 2) AS avg_tempo,
    ROUND(AVG(m.valence), 3) AS avg_valence

FROM music_clean m
LEFT JOIN listening_clean l ON m.track_id = l.track_id
WHERE m.genre != 'unknown'
  AND m.year > 0
  AND m.year >= 1950
GROUP BY m.genre, m.year
HAVING COUNT(DISTINCT m.track_id) >= 3;

-- Ejemplo: Evolución de géneros principales
SELECT 'EVOLUTION OF MAJOR GENRES (1990-2015)' AS info;
SELECT 
    year,
    genre,
    num_songs,
    total_plays,
    avg_energy,
    avg_tempo
FROM genre_evolution
WHERE genre IN (' alternative', ' indie', 'Rock', ' pop', ' electronic')
  AND year BETWEEN 1990 AND 2015
ORDER BY year, total_plays DESC;


-- ============================================================================
-- ANÁLISIS 3: ARTISTAS EMERGENTES POR DÉCADA
-- ============================================================================

DROP TABLE IF EXISTS artist_trends_by_decade;

CREATE TABLE artist_trends_by_decade
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/trends/artist_by_decade/'
AS
SELECT
    CASE 
        WHEN m.year = 0 THEN 'Unknown'
        WHEN m.year < 1950 THEN 'Pre-1950'
        WHEN m.year BETWEEN 1950 AND 1959 THEN '1950s'
        WHEN m.year BETWEEN 1960 AND 1969 THEN '1960s'
        WHEN m.year BETWEEN 1970 AND 1979 THEN '1970s'
        WHEN m.year BETWEEN 1980 AND 1989 THEN '1980s'
        WHEN m.year BETWEEN 1990 AND 1999 THEN '1990s'
        WHEN m.year BETWEEN 2000 AND 2009 THEN '2000s'
        WHEN m.year >= 2010 THEN '2010s+'
    END AS decade,
    
    m.artist,
    MAX(m.genre) AS primary_genre,
    COUNT(DISTINCT m.track_id) AS num_songs,
    SUM(COALESCE(l.total_playcount, 0)) AS total_plays,
    COUNT(DISTINCT l.user_id) AS unique_listeners,
    ROUND(AVG(COALESCE(l.total_playcount, 0)), 2) AS avg_plays_per_song,
    
    -- Ranking dentro de la década
    ROW_NUMBER() OVER (
        PARTITION BY 
            CASE 
                WHEN m.year = 0 THEN 'Unknown'
                WHEN m.year < 1950 THEN 'Pre-1950'
                WHEN m.year BETWEEN 1950 AND 1959 THEN '1950s'
                WHEN m.year BETWEEN 1960 AND 1969 THEN '1960s'
                WHEN m.year BETWEEN 1970 AND 1979 THEN '1970s'
                WHEN m.year BETWEEN 1980 AND 1989 THEN '1980s'
                WHEN m.year BETWEEN 1990 AND 1999 THEN '1990s'
                WHEN m.year BETWEEN 2000 AND 2009 THEN '2000s'
                WHEN m.year >= 2010 THEN '2010s+'
            END
        ORDER BY SUM(COALESCE(l.total_playcount, 0)) DESC
    ) AS popularity_rank

FROM music_clean m
LEFT JOIN listening_clean l ON m.track_id = l.track_id
WHERE m.year > 0
GROUP BY 
    CASE 
        WHEN m.year = 0 THEN 'Unknown'
        WHEN m.year < 1950 THEN 'Pre-1950'
        WHEN m.year BETWEEN 1950 AND 1959 THEN '1950s'
        WHEN m.year BETWEEN 1960 AND 1969 THEN '1960s'
        WHEN m.year BETWEEN 1970 AND 1979 THEN '1970s'
        WHEN m.year BETWEEN 1980 AND 1989 THEN '1980s'
        WHEN m.year BETWEEN 1990 AND 1999 THEN '1990s'
        WHEN m.year BETWEEN 2000 AND 2009 THEN '2000s'
        WHEN m.year >= 2010 THEN '2010s+'
    END,
    m.artist
HAVING COUNT(DISTINCT m.track_id) >= 3;

-- Top 15 artistas por década
SELECT 'TOP 15 ARTISTS PER DECADE' AS info;
SELECT 
    decade,
    artist,
    primary_genre,
    num_songs,
    total_plays,
    unique_listeners,
    popularity_rank
FROM artist_trends_by_decade
WHERE popularity_rank <= 15
ORDER BY decade, popularity_rank;


-- ============================================================================
-- ANÁLISIS 4: ONE-HIT WONDERS vs ARTISTAS CONSISTENTES
-- ============================================================================

DROP TABLE IF EXISTS artist_consistency;

CREATE TABLE artist_consistency
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/trends/artist_consistency/'
AS
SELECT
    m.artist,
    COUNT(DISTINCT m.track_id) AS total_songs,
    SUM(COALESCE(l.total_playcount, 0)) AS total_plays,
    MAX(COALESCE(l.total_playcount, 0)) AS max_song_plays,
    ROUND(AVG(COALESCE(l.total_playcount, 0)), 2) AS avg_plays_per_song,
    ROUND(STDDEV_POP(COALESCE(l.total_playcount, 0)), 2) AS stddev_plays,
    
    -- Ratio: plays de la canción top / plays promedio
    ROUND(
        MAX(COALESCE(l.total_playcount, 0)) / 
        NULLIF(AVG(COALESCE(l.total_playcount, 0)), 0),
        2
    ) AS hit_concentration_ratio,
    
    -- Clasificación
    CASE
        WHEN COUNT(DISTINCT m.track_id) = 1 
             AND MAX(COALESCE(l.total_playcount, 0)) > 100 
        THEN 'One-Hit Wonder'
        
        WHEN COUNT(DISTINCT m.track_id) >= 2 
             AND MAX(COALESCE(l.total_playcount, 0)) / NULLIF(AVG(COALESCE(l.total_playcount, 0)), 0) > 5 
        THEN 'Hit-Driven Artist'
        
        WHEN COUNT(DISTINCT m.track_id) >= 5 
             AND STDDEV_POP(COALESCE(l.total_playcount, 0)) < AVG(COALESCE(l.total_playcount, 0)) 
        THEN 'Consistent Artist'
        
        ELSE 'Regular Artist'
    END AS artist_type

FROM music_clean m
LEFT JOIN listening_clean l ON m.track_id = l.track_id
GROUP BY m.artist
HAVING SUM(COALESCE(l.total_playcount, 0)) > 0;

-- Distribución de tipos de artistas
SELECT 'ARTIST TYPE DISTRIBUTION' AS info;
SELECT 
    artist_type,
    COUNT(*) AS num_artists,
    ROUND(AVG(total_plays), 2) AS avg_total_plays,
    ROUND(AVG(total_songs), 2) AS avg_songs_per_artist
FROM artist_consistency
GROUP BY artist_type
ORDER BY num_artists DESC;

-- Top One-Hit Wonders
SELECT 'TOP 20 ONE-HIT WONDERS' AS info;
SELECT 
    artist,
    total_songs,
    max_song_plays,
    artist_type
FROM artist_consistency
WHERE artist_type = 'One-Hit Wonder'
ORDER BY max_song_plays DESC
LIMIT 20;

-- ============================================================================
-- ANÁLISIS 5: DIVERSIDAD MUSICAL POR DÉCADA
-- ============================================================================

DROP TABLE IF EXISTS musical_diversity;

CREATE TABLE musical_diversity
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/trends/musical_diversity/'
AS
SELECT
    decade,
    COUNT(DISTINCT genre) AS num_genres,
    SUM(total_songs) AS num_songs,
    ROUND(-SUM((cnt_genre / total_decade_songs) * LOG2(cnt_genre / total_decade_songs)), 3) AS genre_diversity_index
FROM (
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
        genre,
        COUNT(*) AS cnt_genre,
        COUNT(*) AS total_songs,
        SUM(COUNT(*)) OVER (PARTITION BY 
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
        ) AS total_decade_songs
    FROM music_clean
    WHERE genre != 'unknown' AND year > 0
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
        END,
        genre,
        year
) subq
GROUP BY decade
ORDER BY decade;

SELECT 'MUSICAL DIVERSITY BY DECADE' AS info;
SELECT * FROM musical_diversity ORDER BY decade;
-- ============================================================================
-- ANÁLISIS 6: GÉNEROS EN DECLIVE Y EMERGENTES
-- ============================================================================

DROP TABLE IF EXISTS genre_momentum;

CREATE TABLE genre_momentum
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/trends/genre_momentum/'
AS
SELECT
    genre,
    
    -- Plays en décadas recientes
    SUM(CASE WHEN year BETWEEN 1990 AND 1999 THEN total_plays ELSE 0 END) AS plays_1990s,
    SUM(CASE WHEN year BETWEEN 2000 AND 2009 THEN total_plays ELSE 0 END) AS plays_2000s,
    SUM(CASE WHEN year >= 2010 THEN total_plays ELSE 0 END) AS plays_2010s,
    
    -- Canciones en décadas recientes
    SUM(CASE WHEN year BETWEEN 1990 AND 1999 THEN num_songs ELSE 0 END) AS songs_1990s,
    SUM(CASE WHEN year BETWEEN 2000 AND 2009 THEN num_songs ELSE 0 END) AS songs_2000s,
    SUM(CASE WHEN year >= 2010 THEN num_songs ELSE 0 END) AS songs_2010s,
    
    -- Crecimiento
    ROUND(
        (SUM(CASE WHEN year >= 2010 THEN total_plays ELSE 0 END) - 
         SUM(CASE WHEN year BETWEEN 2000 AND 2009 THEN total_plays ELSE 0 END)) * 100.0 /
        NULLIF(SUM(CASE WHEN year BETWEEN 2000 AND 2009 THEN total_plays ELSE 0 END), 0),
        2
    ) AS growth_2000s_to_2010s,
    
    -- Clasificación
    CASE
        WHEN (SUM(CASE WHEN year >= 2010 THEN total_plays ELSE 0 END) - 
              SUM(CASE WHEN year BETWEEN 2000 AND 2009 THEN total_plays ELSE 0 END)) * 100.0 /
             NULLIF(SUM(CASE WHEN year BETWEEN 2000 AND 2009 THEN total_plays ELSE 0 END), 0) > 50
        THEN 'Emerging'
        
        WHEN (SUM(CASE WHEN year >= 2010 THEN total_plays ELSE 0 END) - 
              SUM(CASE WHEN year BETWEEN 2000 AND 2009 THEN total_plays ELSE 0 END)) * 100.0 /
             NULLIF(SUM(CASE WHEN year BETWEEN 2000 AND 2009 THEN total_plays ELSE 0 END), 0) < -20
        THEN 'Declining'
        
        ELSE 'Stable'
    END AS momentum

FROM genre_evolution
WHERE year >= 1990
GROUP BY genre
HAVING SUM(CASE WHEN year BETWEEN 2000 AND 2009 THEN total_plays ELSE 0 END) > 100;

-- Géneros emergentes
SELECT 'TOP 15 EMERGING GENRES' AS info;
SELECT 
    genre,
    plays_2000s,
    plays_2010s,
    growth_2000s_to_2010s,
    momentum
FROM genre_momentum
WHERE momentum = 'Emerging'
ORDER BY growth_2000s_to_2010s DESC
LIMIT 15;

-- Géneros en declive
SELECT 'TOP 15 DECLINING GENRES' AS info;
SELECT 
    genre,
    plays_2000s,
    plays_2010s,
    growth_2000s_to_2010s,
    momentum
FROM genre_momentum
WHERE momentum = 'Declining'
ORDER BY growth_2000s_to_2010s ASC
LIMIT 15;


-- ============================================================================
-- RESUMEN FINAL
-- ============================================================================

SELECT '====== JOB 4 COMPLETED ======' AS status;

SELECT 'Tables created' AS metric, '6' AS value
UNION ALL
SELECT 'Genre trends by decade', CAST(COUNT(*) AS STRING) FROM genre_trends_by_decade
UNION ALL
SELECT 'Genre evolution records', CAST(COUNT(*) AS STRING) FROM genre_evolution
UNION ALL
SELECT 'Artist trends by decade', CAST(COUNT(*) AS STRING) FROM artist_trends_by_decade
UNION ALL
SELECT 'Artist consistency profiles', CAST(COUNT(*) AS STRING) FROM artist_consistency
UNION ALL
SELECT 'Musical diversity records', CAST(COUNT(*) AS STRING) FROM musical_diversity
UNION ALL
SELECT 'Genre momentum analysis', CAST(COUNT(*) AS STRING) FROM genre_momentum;

-- ============================================================================
-- FIN DEL JOB 4
-- ============================================================================
