WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        CAST(is_remote AS INT) AS is_remote_flag
    FROM employees
    WHERE hire_date > DATE '2023-09-04'
      AND salary < 132000
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(salary) OVER w_hist AS last4_max_salary,
        MIN(salary) OVER w_hist AS last4_min_salary,
        AVG(salary) OVER w_hist AS last4_avg_salary,
        COUNT(salary) OVER w_hist AS last4_count_salary
    FROM cleaned
    WHERE level != 'L2'
    WINDOW w_hist AS (PARTITION BY level ORDER BY is_remote_flag, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY is_remote_flag, id) AS seq_no,
        AVG(last4_avg_salary) OVER w_part AS last4_avg_salary_avg_3
    FROM with_windows
    WINDOW w_part AS (ORDER BY last4_min_salary, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        job_title,
        salary,
        hire_date,
        is_remote,
        last4_avg_salary,
        last4_count_salary,
        seq_no,
        last4_avg_salary_avg_3,
        ROUND(last4_avg_salary / SUM(last4_avg_salary) OVER (PARTITION BY level), 4) AS last4_avg_salary_share,
        FIRST_VALUE(level, TRUE) OVER w_part AS first_known_level
    FROM scored
    WINDOW w_part AS (PARTITION BY job_title ORDER BY last4_max_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    seq_no,
    COUNT(*) FILTER (WHERE job_title = 'Engineer') AS engineer_rows,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN job_title = 'Manager' THEN 1 END), 0), 2) AS salary_per_manager
FROM enriched
GROUP BY seq_no
ORDER BY engineer_rows NULLS LAST;
