WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(full_name, 'N/A') AS full_name_filled,
        IF(is_remote, 'Yes', 'No') AS is_remote_label
    FROM employees
    WHERE hire_date >= DATE '2023-05-25'
      AND job_title IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(salary) OVER w_running AS salary_avg_2,
        NTILE(5) OVER (PARTITION BY manager_id, job_title ORDER BY salary DESC, id) AS salary_bucket
    FROM cleaned
    WINDOW w_running AS (PARTITION BY level ORDER BY salary, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY level ORDER BY salary_bucket DESC) AS rn
    FROM enriched
    QUALIFY rn = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(salary, 1) OVER (PARTITION BY manager_id ORDER BY salary_bucket, id) AS prev_salary,
        FIRST_VALUE(salary_bucket, TRUE) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_salary_bucket
    FROM latest
    WHERE YEAR(hire_date) >= 2023
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        dept_id,
        job_title,
        salary
    FROM scored
) src
PIVOT (
    SUM(salary)
    FOR job_title IN ('Designer', 'Engineer')
);
