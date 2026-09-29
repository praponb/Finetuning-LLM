WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        CAST(is_remote AS INT) AS is_remote_flag,
        CASE WHEN salary >= 120000 THEN 'High' WHEN salary >= 86000 THEN 'Medium' ELSE 'Low' END AS salary_level
    FROM employees
    WHERE hire_date > TO_DATE('2024-01-01')
      AND level IS NOT NULL
      AND job_title <> 'Engineer'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(salary) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS salary_avg_6,
        FIRST_VALUE(is_remote_flag) IGNORE NULLS OVER w_recent AS first_known_is_remote_flag,
        AVG(salary) OVER w_running AS salary_avg_5
    FROM base
    WINDOW w_recent AS (PARTITION BY manager_id, level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY dept_id, manager_id ORDER BY salary, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(salary_avg_6) OVER (PARTITION BY manager_id) AS manager_id_avg_salary_avg_6,
        ROW_NUMBER() OVER (PARTITION BY level, dept_id ORDER BY salary_avg_5, id) AS seq_no,
        AVG(first_known_is_remote_flag) OVER (ORDER BY salary_avg_5, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS first_known_is_remote_flag_avg_5
    FROM scored
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN salary >= 139000 THEN 'High' WHEN salary >= 86000 THEN 'Medium' ELSE 'Low' END AS salary_level_2
    FROM metrics
    WHERE hire_date >= DATE '2023-08-05'
      AND level IS NOT NULL
),
scored_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER w_hist AS seq_no_dense_rank,
        SUM(salary_avg_6) OVER w_hist_2 AS running_salary_avg_6,
        NTILE(5) OVER (ORDER BY is_remote_flag, id) AS is_remote_flag_bucket,
        SUM(first_known_is_remote_flag) OVER w_recent AS cumulative_first_known_is_remote_flag
    FROM filtered
    WINDOW w_hist AS (PARTITION BY dept_id, level ORDER BY seq_no NULLS FIRST), w_hist_2 AS (ORDER BY first_known_is_remote_flag_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY level ORDER BY salary_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, manager_id
    FROM scored_employees
    INTERSECT ALL
    SELECT id, manager_id
    FROM employees
    WHERE hire_date BETWEEN '2023-10-24' AND '2025-06-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored_employees s
    JOIN matched_keys k ON s.id = k.id AND s.manager_id = k.manager_id
)
SELECT
    id,
    full_name,
    dept_id,
    salary,
    hire_date,
    salary_avg_6,
    first_known_is_remote_flag,
    salary_avg_5,
    manager_id_avg_salary_avg_6,
    CASE WHEN full_name RLIKE '^(Alice Wong|Ivy Chen)$' THEN 'CORE' WHEN full_name RLIKE '^(bob smith|  Carol Diaz)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS full_name_zone
FROM kept_rows
ORDER BY salary_avg_5 DESC NULLS LAST, dept_id DESC;
