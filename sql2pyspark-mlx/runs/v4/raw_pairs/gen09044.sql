WITH valid AS (
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        CAST(is_remote AS INT) AS is_remote_flag
    FROM employees
    WHERE level IS NOT NULL
      AND dept_id IN (10, 16, 24, 32)
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY hire_date DESC, id) AS event_seq,
        LEAD(is_remote_flag, 2) OVER (PARTITION BY level, manager_id ORDER BY is_remote_flag, id) AS next_is_remote_flag,
        NTILE(3) OVER w_recent AS is_remote_flag_bucket
    FROM valid
    WINDOW w_recent AS (PARTITION BY manager_id ORDER BY is_remote_flag DESC, id)
),
valid_employees AS (
    SELECT
        id,
        full_name,
        email,
        manager_id,
        hire_date,
        event_seq,
        is_remote_flag_bucket,
        CAST(is_remote AS INT) AS is_remote_flag_2
    FROM scored
    WHERE hire_date >= DATE '2023-12-11'
),
remaining_keys AS (
    SELECT id
    FROM valid_employees
    EXCEPT
    SELECT id
    FROM employees
    WHERE email IS NOT NULL
),
reconciled AS (
    SELECT s.*
    FROM valid_employees s
    JOIN remaining_keys k ON s.id = k.id
),
valid_employees_2 AS (
    SELECT
        *,
        CASE WHEN is_remote_flag_2 >= 73 THEN 'High' WHEN is_remote_flag_2 >= 14 THEN 'Medium' ELSE 'Low' END AS is_remote_flag_2_level,
        SPLIT(LOWER(email), '@')[0] AS email_user
    FROM reconciled
    WHERE manager_id IS NOT NULL
),
with_dims AS (
    SELECT
        a.*,
        b.full_name AS employee_full_name,
        b.salary,
        b.hire_date AS employee_hire_date
    FROM valid_employees_2 a
    LEFT JOIN employees b
        ON a.manager_id = b.id
),
valid_employees_3 AS (
    SELECT
        id,
        email,
        manager_id,
        hire_date,
        is_remote_flag_bucket,
        is_remote_flag_2,
        is_remote_flag_2_level,
        employee_full_name,
        salary,
        ROUND(is_remote_flag_2 * 1.15, 2) AS is_remote_flag_2_scaled
    FROM with_dims
    WHERE hire_date >= DATE '2023-06-12'
)
SELECT
    id,
    email,
    manager_id,
    hire_date,
    is_remote_flag_bucket,
    is_remote_flag_2,
    is_remote_flag_2_level,
    employee_full_name,
    salary,
    CASE WHEN employee_full_name RLIKE '^(  Carol Diaz|Emma Brown)$' THEN 'PRIORITY' WHEN employee_full_name RLIKE '^(Alice Wong|bob smith)$' THEN 'CORE' ELSE 'GROUP_A' END AS employee_full_name_zone
FROM valid_employees_3
ORDER BY salary NULLS LAST;
