WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        INITCAP(TRIM(level)) AS level_title,
        COALESCE(job_title, 'UNKNOWN') AS resolved_job_title
    FROM employees
    WHERE level IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id, dept_id ORDER BY hire_date, id) = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        level_title,
        AVG(salary) OVER w_running AS last7_avg_salary,
        SUM(salary) OVER w_running AS last7_sum_salary,
        COUNT(salary) OVER w_running AS last7_count_salary,
        NTILE(4) OVER w_running_2 AS salary_bucket
    FROM ranked
    WINDOW w_running AS (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_running_2 AS (ORDER BY salary, id)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(last7_count_salary) IGNORE NULLS OVER (PARTITION BY dept_id ORDER BY last7_avg_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last7_count_salary,
        LAST_VALUE(last7_avg_salary) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY last7_sum_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last7_avg_salary
    FROM enriched
    WHERE salary >= 128000
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY last_known_last7_count_salary DESC NULLS FIRST, id) = 1
)
SELECT
    id,
    full_name,
    dept_id,
    manager_id,
    job_title,
    last7_count_salary,
    salary_bucket,
    last_known_last7_count_salary,
    CASE WHEN full_name RLIKE '^(Pim Chaiyaporn|  Carol Diaz)$' THEN 'EXTENDED' WHEN full_name RLIKE '^(Alice Wong|bob smith)$' THEN 'PRIORITY' ELSE 'CORE' END AS full_name_zone
FROM deduped
ORDER BY job_title DESC NULLS FIRST, last7_count_salary;
