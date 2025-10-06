-- Test básico de Hive
SHOW DATABASES;
CREATE DATABASE IF NOT EXISTS test_db;
USE test_db;

-- Test tabla simple
CREATE TABLE IF NOT EXISTS test_table (id INT, name STRING);
INSERT INTO test_table VALUES (1, 'test');
SELECT * FROM test_table;

-- Test lectura de S3
DROP TABLE IF EXISTS music_test;
CREATE EXTERNAL TABLE music_test (
    col1 STRING,
    col2 STRING,
    col3 STRING,
    col4 STRING,
    col5 STRING,
    col6 STRING,
    col7 STRING,
    col8 STRING,
    col9 STRING,
    col10 STRING,
    col11 STRING,
    col12 STRING,
    col13 STRING,
    col14 STRING,
    col15 STRING,
    col16 STRING,
    col17 STRING,
    col18 STRING,
    col19 STRING,
    col20 STRING,
    col21 STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://emr-logs-1758750407/music-data/raw-csv/'
TBLPROPERTIES ("skip.header.line.count"="1");

SELECT COUNT(*) AS total_rows FROM music_test;
SELECT * FROM music_test LIMIT 3;
