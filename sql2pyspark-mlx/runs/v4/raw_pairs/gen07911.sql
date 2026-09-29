WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_hist AS event_seq,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY salary DESC) AS salary_dense_rank,
        ROW_NUMBER() OVER w_part AS event_seq_2,
        DENSE_RANK() OVER w_hist_2 AS salary_dense_rank_2,
        FIRST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY manager_id, dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title
    FROM employees
    WINDOW w_hist AS (PARTITION BY manager_id ORDER BY hire_date DESC, id), w_part AS (ORDER BY salary, id), w_hist_2 AS (PARTITION BY level, dept_id ORDER BY salary DESC)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        GREATEST(salary, salary_dense_rank) AS greatest_salary_salary_dense_rank,
        LENGTH(full_name) AS full_name_length,
        COALESCE(job_title, 'N/A') AS job_title_clean
    FROM scored
    WHERE level IS NOT NULL
      AND salary >= 140000
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY greatest_salary_salary_dense_rank DESC NULLS FIRST, id) = 1
),
joined AS (
    -- Attach departments attributes
    SELECT
        s.*,
        p.location,
        p.dept_name
    FROM latest s
    LEFT JOIN departments p
        ON s.dept_id = p.id
),
scored_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(full_name_length) OVER (PARTITION BY location) AS location_count_full_name_length,
        NTILE(5) OVER (PARTITION BY job_title, level ORDER BY event_seq_2, id) AS event_seq_2_bucket,
        LAST_VALUE(greatest_salary_salary_dense_rank, TRUE) OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_greatest_salary_salary_dense_rank
    FROM joined
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT manager_id, id
    FROM scored_employees
    INTERSECT
    SELECT manager_id, id
    FROM employees
    WHERE hire_date <= '2024-06-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored_employees s
    INNER JOIN remaining_keys k ON s.manager_id = k.manager_id AND s.id = k.id
)
-- Running and moving aggregates
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
    level,
    event_seq,
    salary_dense_rank,
    event_seq_2,
    salary_dense_rank_2,
    first_known_job_title,
    greatest_salary_salary_dense_rank,
    full_name_length,
    job_title_clean,
    location,
    dept_name,
    location_count_full_name_length,
    event_seq_2_bucket,
    last_known_greatest_salary_salary_dense_rank,
    COUNT(full_name_length) OVER (PARTITION BY dept_name) AS dept_name_count_full_name_length,
    ROUND(salary_dense_rank / SUM(salary_dense_rank) OVER (PARTITION BY job_title), 4) AS salary_dense_rank_share,
    MIN(salary) OVER (PARTITION BY job_title) AS job_title_min_salary
FROM reconciled
