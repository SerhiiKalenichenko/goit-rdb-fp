-- =====================================================
-- TASK 1: Створення схеми та перевірка імпорту даних
-- =====================================================

CREATE SCHEMA IF NOT EXISTS pandemic;
USE pandemic;

SELECT *
FROM infectious_cases
LIMIT 10;

SELECT COUNT(*) AS imported_rows_count
FROM infectious_cases;


-- =====================================================
-- TASK 2: Нормалізація до 3NF
-- =====================================================

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
    TRIM(Entity),
    NULLIF(TRIM(Code), '')
FROM infectious_cases
WHERE NULLIF(TRIM(Entity), '') IS NOT NULL;

CREATE TABLE infectious_cases_normalized (
    record_id INT NOT NULL AUTO_INCREMENT,
    entity_id INT NOT NULL,
    Year INT NOT NULL,
    Number_yaws VARCHAR(50),
    polio_cases VARCHAR(50),
    cases_guinea_worm VARCHAR(50),
    Number_rabies VARCHAR(50),
    Number_malaria VARCHAR(50),
    Number_hiv VARCHAR(50),
    Number_tuberculosis VARCHAR(50),
    Number_smallpox VARCHAR(50),
    Number_cholera_cases VARCHAR(50),
    PRIMARY KEY (record_id),
    CONSTRAINT fk_entity
        FOREIGN KEY (entity_id)
        REFERENCES entities(entity_id)
);

INSERT INTO infectious_cases_normalized (
    entity_id,
    Year,
    Number_yaws,
    polio_cases,
    cases_guinea_worm,
    Number_rabies,
    Number_malaria,
    Number_hiv,
    Number_tuberculosis,
    Number_smallpox,
    Number_cholera_cases
)
SELECT
    e.entity_id,
    CAST(TRIM(ic.Year) AS UNSIGNED),
    ic.Number_yaws,
    ic.polio_cases,
    ic.cases_guinea_worm,
    ic.Number_rabies,
    ic.Number_malaria,
    ic.Number_hiv,
    ic.Number_tuberculosis,
    ic.Number_smallpox,
    ic.Number_cholera_cases
FROM infectious_cases ic
JOIN entities e
    ON TRIM(ic.Entity) = e.entity
   AND NULLIF(TRIM(ic.Code), '') <=> e.code;

CREATE INDEX idx_entity_id
    ON infectious_cases_normalized (entity_id);

CREATE INDEX idx_year
    ON infectious_cases_normalized (Year);


-- =====================================================
-- TASK 3: Аналітика Number_rabies
-- =====================================================

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


-- =====================================================
-- TASK 4: Робота з датами та різницею років
-- =====================================================

SELECT
    Year AS source_year,
    MAKEDATE(Year, 1) AS first_day_of_year,
    CURDATE() AS current_date,
    TIMESTAMPDIFF(
        YEAR,
        MAKEDATE(Year, 1),
        CURDATE()
    ) AS year_difference
FROM infectious_cases_normalized;


-- =====================================================
-- TASK 5: Власна функція
-- =====================================================

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
    get_year_difference(Year) AS year_difference
FROM infectious_cases_normalized;
