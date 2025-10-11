-- ============================================================================
-- JOB 10: PREPARAR DATOS PARA MAPREDUCE - USER ACTIVITY
-- ============================================================================

-- Paso 0: Recrear tabla listening_clean (apunta a datos existentes en S3)
DROP TABLE IF EXISTS listening_clean;
CREATE EXTERNAL TABLE listening_clean (
    user_id STRING,
    track_id STRING,
    total_playcount INT
)
STORED AS PARQUET
LOCATION 's3://emr-logs-1758750407/music-data/cleaned/listening/';

SELECT 'Tables recreated for Job 10' AS status;

DROP TABLE IF EXISTS user_activity_input;

CREATE EXTERNAL TABLE user_activity_input
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
SELECT 'Total records' AS metric, COUNT(*) AS value FROM user_activity_input;
