-- Verificar tablas creadas en Job 1
SHOW TABLES;

-- Info de music_raw
DESCRIBE FORMATTED music_raw;
SELECT COUNT(*) AS total_songs FROM music_raw;

-- Info de listening_raw
DESCRIBE FORMATTED listening_raw;
SELECT COUNT(*) AS total_plays FROM listening_raw;

-- Muestras
SELECT * FROM music_raw LIMIT 5;
SELECT * FROM listening_raw LIMIT 5;
