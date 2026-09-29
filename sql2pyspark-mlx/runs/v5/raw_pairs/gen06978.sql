WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        job_title,
        salary,
        hire_date,
        level,
        COALESCE(job_title, 'Unknown') AS job_title_filled
    FROM employees
    WHERE hire_date >= DATE '2023-03-20'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('/', level, job_title) AS level_job_title_key,
        COALESCE(level, 'DIRECT') AS level_filled
    FROM filtered
    WHERE hire_date >= DATE '2023-05-25'
),
filtered_employees AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(salary, 1) AS salary_rounded,
        COALESCE(job_title, 'DIRECT') AS job_title_filled_2
    FROM base
    WHERE hire_date >= TO_DATE('2023-06-24')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        job_title,
        hire_date,
        job_title_filled,
        level_job_title_key,
        level_filled,
        salary_rounded,
        FIRST_VALUE(salary_rounded, TRUE) OVER (ORDER BY salary_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_salary_rounded,
        SUM(salary) OVER (PARTITION BY level, job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary,
        ROW_NUMBER() OVER (PARTITION BY job_title, level ORDER BY hire_date, id) AS seq_no
    FROM filtered_employees
    WHERE level != 'L1'
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM scored
    EXCEPT
    SELECT id
    FROM employees
    WHERE manager_id IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.id = k.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(job_title, 'N/A') AS job_title_filled_2
    FROM verified
    WHERE job_title IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        job_title,
        job_title_filled,
        level_job_title_key,
        level_filled,
        salary_rounded,
        cumulative_salary,
        seq_no,
        DENSE_RANK() OVER w_hist AS seq_no_dense_rank,
        CUME_DIST() OVER (PARTITION BY job_title ORDER BY cumulative_salary DESC NULLS FIRST) AS cumulative_salary_cume_dist,
        DENSE_RANK() OVER (ORDER BY first_known_salary_rounded DESC) AS first_known_salary_rounded_dense_rank
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY job_title ORDER BY seq_no DESC)
)
SELECT
    job_title,
    job_title_filled,
    level_job_title_key,
    level_filled,
    salary_rounded,
    cumulative_salary,
    seq_no,
    seq_no_dense_rank,
    cumulative_salary_cume_dist
FROM metrics
ORDER BY job_title DESC NULLS FIRST
