WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.is_remote
        )
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.employee_hire_date
    FROM kept s
    JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.location,
        latest.employee_level,
        latest.hire_date
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY hire_date, id) AS rn
    FROM with_latest
    QUALIFY rn = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date < s.hire_date
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        location,
        rn,
        LAST_VALUE(employee_level, TRUE) OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_employee_level,
        ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY hire_date DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY employee_level ORDER BY hire_date DESC, id) AS row_seq,
        LAST_VALUE(location, TRUE) OVER (PARTITION BY location ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_location,
        LAST_VALUE(location, TRUE) OVER (PARTITION BY dept_name ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_location_2
    FROM not_flagged
    WHERE location != 'Tokyo'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        location,
        seq_no,
        row_seq,
        last_known_location
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY row_seq DESC, id) = 1
)
SELECT
    id,
    location,
    seq_no,
    last_known_location
FROM deduped;
