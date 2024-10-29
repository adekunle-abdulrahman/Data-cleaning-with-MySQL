SELECT *
;

-- Remove duplicates
-- standardize the data
-- Null values or blank values
-- Remove any columns or rows





 CREATE TABLE layoffs_staging
LIKE layoff;

SELECT *
FROM layoffs_staging;


INSERT layoffs_staging
SELECT *
FROM layoff;


SELECT *,
row_number() OVER(partition by company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
FROM layoffs_staging;


with duplicate_cte AS
(
SELECT *,
row_number() OVER(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date' , stage
, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
where row_num > 1;

Select *
from layoffs_staging
where company = 'casper';

with duplicate_cte AS
(
SELECT *,
row_number() OVER(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date' , stage
, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
delete
FROM duplicate_cte
where row_num > 1;




CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



Select *
from layoffs_staging;

SELECT *
from layoffs_staging2
WHERE row_num >1;



INSERT INTO layoffs_staging2
SELECT *,
row_number() OVER(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date' , stage
, country, funds_raised_millions) AS row_num
FROM layoffs_staging;




DELETE
from layoffs_staging2
WHERE row_num >1;

SELECT *
from layoffs_staging2
WHERE row_num >1;




--  standardizing data

SELECT company,(TRIM(company))
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET company = (TRIM(company));


select *
from layoffs_staging2
where industry LIKE 'CRYPTO%';


UPDATE layoffs_staging2
SET industry = 'crypto'
where industry LIKE 'CRYPTO%';


SELECT DISTINCT country, TRIM(TRAILING '.'FROM country)
from layoffs_staging2;



UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.'FROM country)
Where country LIKE 'united states%';



select `date`
from layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date (`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


select *
from layoffs_staging2
WHERE total_laid_off is NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = null
WHERE INDUSTRY = ' ';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = ' ';

select *
from layoffs_staging2
where company  like  'Bally%';

 
 
 SELECT company, industry
FROM layoffs_staging2
WHERE industry IS NULL OR industry = ' ';



SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
WHERE (t1.industry IS NULL OR TRIM(t1.industry) = '')
AND t2.industry IS NOT NULL AND TRIM(t2.industry) <> '';

update layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
set t1.industry =t2.industry
WHERE (t1.industry IS NULL OR TRIM(t1.industry) = '')
AND t2.industry IS NOT NULL AND TRIM(t2.industry) <> '';


select *
from layoffs_staging2
WHERE total_laid_off is NULL
AND percentage_laid_off IS NULL;


delete
from layoffs_staging2
WHERE total_laid_off is NULL
AND percentage_laid_off IS NULL;


SELECT *
from layoffs_staging2;


ALTER  TABLE layoffs_staging2
DROP column row_num;