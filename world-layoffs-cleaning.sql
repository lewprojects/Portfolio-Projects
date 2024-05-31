-- Data Cleaning Project

SELECT *
FROM layoffs;

-- CREATE STAGING TABLE

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;


-- STEP 1: REMOVE DUPLICATES
-- FIND DUPLICATES

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper'
;

-- RE-STAGE TO DELETE DUPLICATES

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- STEP 2: STANDARDIZE DATA
-- TRIM SPACE/PUNCTUATION

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- STANDARDIZE SPELLING

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- RE-ASSIGN DATA TYPES

UPDATE layoffs_staging2
SET `date` = 
    CASE
        WHEN `date` IS NOT NULL AND `date` != 'NULL' THEN STR_TO_DATE(`date`, '%m/%d/%Y')
        ELSE NULL  
    END;
    
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

ALTER TABLE layoffs_staging2
MODIFY COLUMN total_laid_off INT;

ALTER TABLE layoffs_staging2
MODIFY COLUMN funds_raised_millions INT;


-- STEP 3: NULL/BLANK VALUES
-- USE EXISTING DATA TO POPULATE MISSING DATA WHERE POSSIBLE

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = 'NULL'
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = 'NULL' OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- SET ALL BLANK VALUES TO NULL

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = 'NULL' OR industry = '';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

-- STEP 4: REMOVE UNECESSARY COLUMNS AND ROWS

SELECT *
FROM layoffs_staging2
WHERE total_laid_off = "NULL"
AND percentage_laid_off = "NULL";

UPDATE layoffs_staging2
SET total_laid_off = NULL
WHERE total_laid_off = "NULL";

UPDATE layoffs_staging2
SET percentage_laid_off = NULL
WHERE percentage_laid_off = "NULL";

UPDATE layoffs_staging2
SET funds_raised_millions = NULL
WHERE funds_raised_millions = "NULL";

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2