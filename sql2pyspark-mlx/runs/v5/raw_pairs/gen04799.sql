WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, IF(is_remote, 'Yes', 'No') AS is_remote_label
    FROM employees
    WHERE salary >= 106000
      AND job_title IS DISTINCT FROM 'Designer'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS event_seq,
        NTILE(5) OVER (PARTITION BY level ORDER BY salary DESC, id) AS salary_bucket
    FROM cleaned
    WHERE dept_id = 27
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, manager_id
    FROM with_windows
    EXCEPT ALL
    SELECT id, manager_id
    FROM employees
    WHERE level = 'L3'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN remaining_keys k ON s.id = k.id AND s.manager_id = k.manager_id
),
joined AS (
    -- Attach employees attributes
    SELECT
        src.*,
        p.hire_date AS employee_hire_date,
        p.level AS employee_level,
        p.job_title AS employee_job_title
    FROM kept_rows src
    LEFT JOIN employees p
        ON src.manager_id = p.id
)
SELECT
    email,
    dept_id,
    manager_id,
    is_remote,
    level,
    event_seq,
    CASE WHEN level RLIKE '^(L3|L2)$' THEN 'GROUP_A' WHEN level RLIKE '^(L1|L4)$' THEN 'CORE' ELSE 'PRIORITY' END AS level_zone
FROM joined
