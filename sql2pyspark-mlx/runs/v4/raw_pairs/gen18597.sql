WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        TRIM(job_title) AS clean_job_title,
        COALESCE(job_title, 'NONE') AS job_title_filled
    FROM employees
    WHERE hire_date > DATE '2024-03-23'
      AND LENGTH(full_name) >= 13
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        clean_job_title,
        job_title_filled,
        MIN(salary) OVER (PARTITION BY manager_id) AS manager_id_min_salary,
        AVG(salary) OVER (PARTITION BY dept_id, manager_id ORDER BY salary ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS salary_avg_3
    FROM base
),
kept_keys AS (
    -- Set intersection on keys
    SELECT manager_id
    FROM metrics
    INTERSECT
    SELECT manager_id
    FROM employees
    WHERE dept_id = 5
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.manager_id = k.manager_id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(manager_id_min_salary / SUM(manager_id_min_salary) OVER (PARTITION BY job_title), 4) AS manager_id_min_salary_share,
        AVG(manager_id_min_salary) OVER (PARTITION BY dept_id) AS dept_id_avg_manager_id_min_salary,
        PERCENT_RANK() OVER (ORDER BY salary ASC) AS salary_percent_rank
    FROM kept_rows
)
-- Window metrics over several different partitions
SELECT
    *,
    COUNT(salary_avg_3) OVER (PARTITION BY dept_id) AS dept_id_count_salary_avg_3,
    RANK() OVER (PARTITION BY job_title ORDER BY salary DESC) AS salary_rank,
    ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share,
    DENSE_RANK() OVER (ORDER BY dept_id_avg_manager_id_min_salary ASC) AS dept_id_avg_manager_id_min_salary_dense_rank,
    PERCENT_RANK() OVER w_ordered AS dept_id_avg_manager_id_min_salary_percent_rank
FROM scored
WINDOW w_ordered AS (PARTITION BY dept_id ORDER BY dept_id_avg_manager_id_min_salary ASC);
