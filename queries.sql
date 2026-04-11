CREATE SCHEMA IF NOT EXISTS pandemic;
USE pandemic;

SELECT *
FROM infectious_cases
LIMIT 10;

SELECT COUNT(*) AS imported_rows_count
FROM infectious_cases;

DROP TABLE IF EXISTS infectious_cases_normalized;
DROP TABLE IF EXISTS entities;

CREATE TABLE entities (
    entity_id INT NOT NULL AUTO_INCREMENT,
    entity VARCHAR(255) NOT NULL,
    code VARCHAR(32) NULL,
    PRIMARY KEY (entity_id),
    UNIQUE KEY uq_entities_entity_code (entity, code)
);

INSERT INTO entities (entity, code)
SELECT DISTINCT
    TRIM(Entity) AS entity,
    NULLIF(TRIM(Code), '') AS code
FROM infectious_cases
WHERE NULLIF(TRIM(Entity), '') IS NOT NULL;

CREATE TABLE infectious_cases_normalized
LIKE infectious_cases;

ALTER TABLE infectious_cases_normalized
    ADD COLUMN record_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST,
    ADD COLUMN entity_id INT NULL AFTER record_id;

INSERT INTO infectious_cases_normalized
SELECT
    NULL AS record_id,
    NULL AS entity_id,
    ic.*
FROM infectious_cases ic;

SET SQL_SAFE_UPDATES = 0;

UPDATE infectious_cases_normalized icn
JOIN entities e
    ON TRIM(icn.Entity) = e.entity
   AND NULLIF(TRIM(icn.Code), '') <=> e.code
SET icn.entity_id = e.entity_id;

SET SQL_SAFE_UPDATES = 1;

ALTER TABLE infectious_cases_normalized
    MODIFY COLUMN entity_id INT NOT NULL,
    ADD CONSTRAINT fk_infectious_cases_normalized_entity
        FOREIGN KEY (entity_id) REFERENCES entities(entity_id);

ALTER TABLE infectious_cases_normalized
    DROP COLUMN Entity,
    DROP COLUMN Code;

CREATE INDEX idx_infectious_cases_normalized_entity_id
    ON infectious_cases_normalized (entity_id);

CREATE INDEX idx_infectious_cases_normalized_year
    ON infectious_cases_normalized (Year);

SELECT
    e.entity_id,
    e.entity,
    e.code,
    AVG(CAST(TRIM(icn.Number_rabies) AS DECIMAL(18,2))) AS avg_number_rabies,
    MIN(CAST(TRIM(icn.Number_rabies) AS DECIMAL(18,2))) AS min_number_rabies,
    MAX(CAST(TRIM(icn.Number_rabies) AS DECIMAL(18,2))) AS max_number_rabies,
    SUM(CAST(TRIM(icn.Number_rabies) AS DECIMAL(18,2))) AS sum_number_rabies
FROM infectious_cases_normalized icn
JOIN entities e
    ON icn.entity_id = e.entity_id
WHERE NULLIF(TRIM(icn.Number_rabies), '') IS NOT NULL
  AND TRIM(icn.Number_rabies) REGEXP '^[0-9]+(\\.[0-9]+)?$'
GROUP BY
    e.entity_id,
    e.entity,
    e.code
ORDER BY avg_number_rabies DESC
LIMIT 10;

SELECT
    Year AS source_year,
    MAKEDATE(CAST(TRIM(Year) AS UNSIGNED), 1) AS first_day_of_year,
    CURDATE() AS current_date,
    TIMESTAMPDIFF(
        YEAR,
        MAKEDATE(CAST(TRIM(Year) AS UNSIGNED), 1),
        CURDATE()
    ) AS year_difference
FROM infectious_cases_normalized
WHERE NULLIF(TRIM(Year), '') IS NOT NULL
  AND TRIM(Year) REGEXP '^[0-9]{4}$';

DROP FUNCTION IF EXISTS get_year_difference;

DELIMITER $$

CREATE FUNCTION get_year_difference(input_year INT)
RETURNS INT
NOT DETERMINISTIC
NO SQL
BEGIN
    RETURN TIMESTAMPDIFF(
        YEAR,
        MAKEDATE(input_year, 1),
        CURDATE()
    );
END $$

DELIMITER ;

SELECT
    Year AS source_year,
    get_year_difference(CAST(TRIM(Year) AS UNSIGNED)) AS year_difference
FROM infectious_cases_normalized
WHERE NULLIF(TRIM(Year), '') IS NOT NULL
  AND TRIM(Year) REGEXP '^[0-9]{4}$';
