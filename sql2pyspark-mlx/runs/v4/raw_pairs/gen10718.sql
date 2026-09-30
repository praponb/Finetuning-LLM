WITH cleaned AS (
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        INITCAP(TRIM(level)) AS level_title,
        ROUND(salary, 2) AS salary_rounded
    FROM employees
    WHERE hire_date >= DATE '2024-02-10'
      AND LOWER(TRIM(email)) = 'liam@outlook.com'
)
SELECT *
FROM (
    SELECT
        id,
        manager_id,
        salary_rounded,
        salary
    FROM cleaned
) src
UNPIVOT (
    value FOR measure IN (salary_rounded, salary)
);
