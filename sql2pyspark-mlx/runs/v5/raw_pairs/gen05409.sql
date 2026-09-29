WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_id,
        salary,
        hire_date,
        is_remote,
        UPPER(level) AS level_upper
    FROM employees
    WHERE level IS NOT NULL
      AND dept_id IN (1, 2, 31, 35)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        salary,
        level_upper
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC NULLS LAST, id) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        level_upper,
        ROW_NUMBER() OVER (ORDER BY salary, id) AS row_seq,
        SUM(salary) OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary,
        ROW_NUMBER() OVER (ORDER BY salary DESC, id) AS event_seq
    FROM deduped
    WHERE salary <= 117000
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM metrics
    INTERSECT
    SELECT id
    FROM employees
    WHERE is_remote
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        row_seq,
        cumulative_salary,
        row_seq - cumulative_salary AS row_seq_minus_cumulative_salary,
        event_seq - row_seq AS event_seq_minus_row_seq
    FROM verified
    WHERE cumulative_salary < 329410
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN row_seq_minus_cumulative_salary >= 70 THEN 'High' WHEN row_seq_minus_cumulative_salary >= 17 THEN 'Medium' ELSE 'Low' END AS row_seq_minus_cumulative_salary_band
    FROM filtered
    WHERE row_seq_minus_cumulative_salary BETWEEN 39 AND 70
),
metrics_employees AS (
    -- Ranking and gap features
    SELECT
        id,
        row_seq,
        cumulative_salary,
        event_seq_minus_row_seq,
        DENSE_RANK() OVER (ORDER BY cumulative_salary NULLS FIRST) AS cumulative_salary_dense_rank,
        RANK() OVER (ORDER BY cumulative_salary DESC NULLS FIRST) AS cumulative_salary_rank
    FROM cleaned
)
SELECT
    id,
    row_seq,
    cumulative_salary,
    event_seq_minus_row_seq
FROM metrics_employees;
