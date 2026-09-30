WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN is_remote THEN 'Yes' ELSE 'No' END is_remote_label,
        CAST(is_remote AS INT) is_remote_flag
    FROM employees
    WHERE hire_date >= DATE '2023-03-24'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY manager_id, level ORDER BY hire_date DESC) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
filtered_employees AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote_label,
        is_remote_flag,
        row_num,
        TRIM(job_title) clean_job_title,
        COALESCE(level, 'UNKNOWN') resolved_level
    FROM deduped
    WHERE hire_date >= DATE '2023-04-18'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote_label,
        is_remote_flag,
        row_num,
        resolved_level,
        row_num + salary row_num_plus_salary
    FROM filtered_employees
    WHERE salary <= 97000
),
filtered_employees_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        row_num / NULLIF(is_remote_flag, 0) row_num_per_is_remote_flag,
        COALESCE(job_title, 'NONE') resolved_job_title
    FROM valid
    WHERE row_num_plus_salary BETWEEN 39 AND 51
),
filtered_employees_3 AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(row_num_per_is_remote_flag AS INT) row_num_per_is_remote_flag_int,
        salary + row_num salary_plus_row_num,
        COALESCE(job_title, 'OTHER') job_title_clean
    FROM filtered_employees_2
    WHERE salary BETWEEN 125000 AND 144000
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(row_num_per_is_remote_flag) OVER (PARTITION BY dept_id, job_title ORDER BY salary_plus_row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_row_num_per_is_remote_flag,
        SUM(row_num_per_is_remote_flag) OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_row_num_per_is_remote_flag_2,
        SUM(salary_plus_row_num) OVER (ORDER BY row_num_per_is_remote_flag_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_salary_plus_row_num
    FROM filtered_employees_3
)
-- Grouped metrics
SELECT
    manager_id,
    CASE WHEN salary_plus_row_num >= 82 THEN 'HIGH' ELSE 'LOW' END AS salary_plus_row_num_tier,
    CASE WHEN cumulative_row_num_per_is_remote_flag_2 >= 391 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_row_num_per_is_remote_flag_2_tier,
    APPROX_PERCENTILE(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
    APPROX_PERCENTILE(cumulative_row_num_per_is_remote_flag, 0.5) AS cumulative_row_num_per_is_remote_flag_p50,
    COUNT(CASE WHEN job_title = 'Intern' THEN 1 END) AS intern_count
FROM with_windows
GROUP BY manager_id, salary_plus_row_num_tier, cumulative_row_num_per_is_remote_flag_2_tier;
