WITH base AS (
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        LENGTH(email) AS email_length,
        ROUND(salary * 0.85, 2) AS salary_scaled,
        COALESCE(level, 'DIRECT') AS level_filled
    FROM employees
    WHERE level IS NOT NULL
),
joined AS (
    SELECT
        src.*,
        b.dept_name,
        b.budget
    FROM base src
    JOIN departments b
        ON src.dept_id = b.id
),
joined_employees AS (
    SELECT e.*, d.level AS employee_level
    FROM joined e
    LEFT JOIN employees d
        ON e.manager_id = d.id
),
scored AS (
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        email_length,
        salary_scaled,
        level_filled,
        dept_name,
        budget,
        employee_level,
        NTILE(3) OVER (PARTITION BY employee_level ORDER BY salary_scaled, id) AS salary_scaled_bucket,
        AVG(email_length) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS email_length_avg_5
    FROM joined_employees
),
cleaned AS (
    SELECT
        *,
        salary_scaled_bucket / NULLIF(email_length, 0) AS salary_scaled_bucket_per_email_length,
        COALESCE(dept_name, 'OTHER') AS dept_name_clean
    FROM scored
    WHERE level IS NOT NULL
      AND salary_scaled_bucket >= 8
)
SELECT *
FROM (
    SELECT
        employee_level,
        manager_id,
        dept_name,
        salary_scaled_bucket
    FROM cleaned
) src
PIVOT (
    SUM(salary_scaled_bucket) AS value_sum,
    COUNT(1) AS item_count
    FOR dept_name IN ('HR', 'Finance')
);
