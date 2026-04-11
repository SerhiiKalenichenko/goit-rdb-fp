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
    code VARCHAR(32) NOT NULL DEFAULT '',
    PRIMARY KEY (entity_id),
    UNIQUE KEY uq_entities_entity_code (entity, code)
);

INSERT INTO entities (entity, code)
SELECT DISTINCT
    TRIM(Entity) AS entity,
    COALESCE(NULLIF(TRIM(Code), ''), '') AS code
FROM infectious_cases
WHERE NULLIF(TRIM(Entity), '') IS NOT NULL;

CREATE TABLE infectious_cases_normalized (
    record_id INT NOT NULL AUTO_INCREMENT,
    entity_id INT NOT NULL,
    year_value INT NOT NULL,
    number_yaws DECIMAL(20,2) NULL,
    polio_cases DECIMAL(20,2) NULL,
    cases_guinea_worm DECIMAL(20,2) NULL,
    number_rabies DECIMAL(20,2) NULL,
    number_malaria DECIMAL(20,2) NULL,
    number_hiv DECIMAL(20,2) NULL,
    number_tuberculosis DECIMAL(20,2) NULL,
    number_smallpox DECIMAL(20,2) NULL,
    number_cholera_cases DECIMAL(20,2) NULL,
    PRIMARY KEY (record_id),
    CONSTRAINT fk_infectious_cases_normalized_entity
        FOREIGN KEY (entity_id) REFERENCES entities(entity_id),
    CONSTRAINT chk_year_value
        CHECK (year_value >= 0)
);

INSERT INTO infectious_cases_normalized (
    entity_id,
    year_value,
    number_yaws,
    polio_cases,
    cases_guinea_worm,
    number_rabies,
    number_malaria,
    number_hiv,
    number_tuberculosis,
    number_smallpox,
    number_cholera_cases
)
SELECT
    e.entity_id,
    CAST(TRIM(ic.Year) AS UNSIGNED) AS year_value,
    CAST(NULLIF(TRIM(ic.Number_yaws), '') AS DECIMAL(20,2)) AS number_yaws,
    CAST(NULLIF(TRIM(ic.polio_cases), '') AS DECIMAL(20,2)) AS polio_cases,
    CAST(NULLIF(TRIM(ic.cases_guinea_worm), '') AS DECIMAL(20,2)) AS cases_guinea_worm,
    CAST(NULLIF(TRIM(ic.Number_rabies), '') AS DECIMAL(20,2)) AS number_rabies,
    CAST(NULLIF(TRIM(ic.Number_malaria), '') AS DECIMAL(20,2)) AS number_malaria,
    CAST(NULLIF(TRIM(ic.Number_hiv), '') AS DECIMAL(20,2)) AS number_hiv,
    CAST(NULLIF(TRIM(ic.Number_tuberculosis), '') AS DECIMAL(20,2)) AS number_tuberculosis,
    CAST(NULLIF(TRIM(ic.Number_smallpox), '') AS DECIMAL(20,2)) AS number_smallpox,
    CAST(NULLIF(TRIM(ic.Number_cholera_cases), '') AS DECIMAL(20,2)) AS number_cholera_cases
FROM infectious_cases ic
JOIN entities e
    ON TRIM(ic.Entity) = e.entity
   AND COALESCE(NULLIF(TRIM(ic.Code), ''), '') = e.code
WHERE NULLIF(TRIM(ic.Year), '') IS NOT NULL;

CREATE INDEX idx_icn_entity_id ON infectious_cases_normalized(entity_id);
CREATE INDEX idx_icn_year_value ON infectious_cases_normalized(year_value);

-- =====================================================
-- TASK 3: Аналітика Number_rabies
-- =====================================================

SELECT
    e.entity_id,
    e.entity,
    e.code,
    ROUND(AVG(icn.number_rabies), 2) AS avg_number_rabies,
    MIN(icn.number_rabies) AS min_number_rabies,
    MAX(icn.number_rabies) AS max_number_rabies,
    ROUND(SUM(icn.number_rabies), 2) AS sum_number_rabies
FROM infectious_cases_normalized icn
JOIN entities e
    ON icn.entity_id = e.entity_id
WHERE icn.number_rabies IS NOT NULL
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
    year_value,
    MAKEDATE(year_value, 1) AS first_day_of_year,
    CURDATE() AS `current_date`,
    TIMESTAMPDIFF(YEAR, MAKEDATE(year_value, 1), CURDATE()) AS year_difference
FROM infectious_cases_normalized
ORDER BY year_value
LIMIT 20;



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
    RETURN TIMESTAMPDIFF(YEAR, MAKEDATE(input_year, 1), CURDATE());
END $$

DELIMITER ;

SELECT
    year_value,
    get_year_difference(year_value) AS year_difference
FROM infectious_cases_normalized
ORDER BY year_value
LIMIT 20;
