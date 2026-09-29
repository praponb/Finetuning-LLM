WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        level,
        FLOOR(salary) AS salary_whole
    FROM employees
    WHERE email LIKE '%@mail.co.th'
      AND manager_id IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        job_title,
        salary,
        level,
        ROUND(salary_whole, 1) AS salary_whole_rounded,
        COALESCE(level, 'N/A') AS level_filled
    FROM valid
    WHERE hire_date >= DATE '2023-04-21'
      AND dept_id IN (13, 34, 37)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        salary,
        level,
        SUM(salary) OVER (PARTITION BY level ORDER BY salary_whole_rounded, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_salary,
        AVG(salary) OVER (PARTITION BY level ORDER BY salary_whole_rounded, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_salary,
        MAX(salary) OVER (PARTITION BY level ORDER BY salary_whole_rounded, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_salary
    FROM cleaned
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        email,
        last3_sum_salary,
        last3_avg_salary
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY last3_sum_salary DESC, id) AS rn
        FROM scored
    ) x
    WHERE rn = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN last3_sum_salary >= 628000 THEN 'PREMIUM' ELSE 'STANDARD' END AS last3_sum_salary_tier,
    APPROX_PERCENTILE(last3_sum_salary, 0.5) AS last3_sum_salary_p50,
    COUNT(*) AS num_rows,
    COUNT(*) AS row_count
FROM ranked
GROUP BY 1
HAVING num_rows > 1
