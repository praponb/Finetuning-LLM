WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(email)) AS email_title,
        UPPER(email) AS email_upper
    FROM employees
    WHERE salary BETWEEN 130000 AND 148000
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        hire_date,
        is_remote,
        level,
        email_title,
        LAG(salary) OVER w_recent AS prev_salary,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM valid
    WINDOW w_recent AS (PARTITION BY level, manager_id ORDER BY salary ASC, id ASC)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY row_seq ASC, id ASC) AS row_num
        FROM scored
    ) x
    WHERE row_num = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY hire_date ASC, id ASC) AS event_seq,
        RANK() OVER (ORDER BY prev_salary DESC) AS prev_salary_rank,
        LEAD(row_seq, 1, 0) OVER (PARTITION BY level ORDER BY prev_salary ASC, id ASC) AS next_row_seq,
        NTILE(10) OVER (ORDER BY prev_salary ASC, id ASC) AS prev_salary_bucket,
        ROW_NUMBER() OVER (ORDER BY prev_salary ASC, id ASC) AS seq_no
    FROM ranked
),
valid_employees AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        is_remote,
        level,
        email_title,
        prev_salary,
        event_seq,
        prev_salary_rank,
        next_row_seq,
        prev_salary_bucket,
        COALESCE(next_row_seq, 0) AS next_row_seq_filled,
        COALESCE(level, 'N/A') AS resolved_level
    FROM metrics
    WHERE hire_date >= DATE '2023-08-13'
)
SELECT
    id,
    full_name,
    is_remote,
    prev_salary,
    prev_salary_rank,
    next_row_seq,
    prev_salary_bucket,
    CASE WHEN full_name RLIKE '^(Mia Rossi|Kanya Srisuk)$' THEN 'PRIORITY' WHEN full_name RLIKE '^(Alice Wong|bob smith)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS full_name_zone
FROM valid_employees
ORDER BY id DESC NULLS FIRST;
