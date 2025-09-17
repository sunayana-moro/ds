create database project1;
use project1;
SET GLOBAL local_infile = 1;
drop table if exists stg_pan_numbers_dataset;
create table stg_pan_numbers_dataset
(
	Pan_Numbers varchar(100)
);
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Copy of PAN Number Validation Dataset.csv'
INTO TABLE stg_pan_numbers_dataset
FIELDS TERMINATED BY ''
LINES TERMINATED BY '\r\n';

select * from stg_pan_numbers_dataset;
select * from stg_pan_numbers_dataset where Pan_Numbers is null; 

select Pan_Numbers, count(1) 
from stg_pan_numbers_dataset 
where Pan_Numbers is not null
group by Pan_Numbers
having count(1) > 1;
select distinct * from stg_pan_numbers_dataset;

select * from stg_pan_numbers_dataset 
where Pan_Numbers <> trim(Pan_Numbers);

select * from stg_pan_numbers_dataset
where binary Pan_Numbers <> binary upper(Pan_Numbers);

drop table  pan_numbers_dataset_cleaned;
create table pan_numbers_dataset_cleaned as
select distinct upper(trim(Pan_Numbers)) as Pan_Numbers
from stg_pan_numbers_dataset 
where Pan_Numbers is not null
and TRIM(Pan_Numbers) <> '';

select * from pan_numbers_dataset_cleaned;

DELIMITER $$
CREATE FUNCTION fn_check_adjacent_repetition(p_str VARCHAR(255))
RETURNS BOOLEAN
DETERMINISTIC
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= LENGTH(p_str) - 1 DO
        IF SUBSTRING(p_str, i, 1) = SUBSTRING(p_str, i + 1, 1) THEN
            RETURN TRUE;
        END IF;
        SET i = i + 1;
    END WHILE;
    RETURN FALSE;
END$$
DELIMITER ;

select fn_check_adjacent_repetition('shlok');

DROP FUNCTION IF EXISTS fn_check_sequential;

DELIMITER $$
CREATE FUNCTION fn_check_sequential(p_str VARCHAR(255))
RETURNS BOOLEAN
DETERMINISTIC
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= LENGTH(p_str) - 1 DO
        IF (((ascii(SUBSTRING(p_str, i, 1)))+1) = (ascii(SUBSTRING(p_str, i + 1, 1)))) THEN
            RETURN TRUE;
        END IF;
        SET i = i + 1;
    END WHILE;
    RETURN FALSE;
END$$
DELIMITER ;

select fn_check_sequential('jmgoap');

DROP TABLE IF EXISTS yv_valid_invalid_pans;
DROP TABLE IF EXISTS cte_cleaned_pan;
DROP TABLE IF EXISTS cte_valid_pan;
-- This is a workaround for lack of WITH clause support in CREATE VIEW

CREATE TABLE cte_cleaned_pan AS
SELECT
  DISTINCT UPPER(TRIM(Pan_Numbers)) AS Pan_Numbers
FROM stg_pan_numbers_dataset
WHERE
  Pan_Numbers IS NOT NULL AND TRIM(Pan_Numbers) <> '';
  
  select * from cte_cleaned_pan;

CREATE TABLE cte_valid_pan AS
SELECT
  *
FROM cte_cleaned_pan
WHERE
  fn_check_adjacent_repetition(Pan_Numbers) = FALSE
  AND fn_check_sequential(SUBSTRING(Pan_Numbers, 1, 5)) = FALSE
  AND fn_check_sequential(SUBSTRING(Pan_Numbers, 6, 4)) = FALSE
  AND Pan_Numbers REGEXP '^([A-Z]){5}([0-9]){4}([A-Z]){1}$';

select * from cte_valid_pan;

CREATE TABLE yv_valid_invalid_pans AS
SELECT
  cln.Pan_Numbers,
  CASE
    WHEN vld.Pan_Numbers IS NULL
    THEN 'Invalid PAN'
    ELSE 'Valid PAN'
  END
  AS status
FROM cte_cleaned_pan cln
LEFT JOIN cte_valid_pan vld ON vld.Pan_Numbers = cln.Pan_Numbers;

select * from yv_valid_invalid_pans;

SELECT
  (
    SELECT
      COUNT(*)
    FROM stg_pan_numbers_dataset
  ) AS total_processed_records,
  (
    SELECT
      COUNT(*)
    FROM yv_valid_invalid_pans
    WHERE
      status = 'Valid PAN'
  ) AS total_valid_pans,
  (
    SELECT
      COUNT(*)
    FROM yv_valid_invalid_pans
    WHERE
      status = 'Invalid PAN'
  ) AS total_invalid_pans,
  (
    SELECT
      (SELECT COUNT(*) FROM stg_pan_numbers_dataset) - (SELECT COUNT(*) FROM yv_valid_invalid_pans)
) AS total_incomplete_pans