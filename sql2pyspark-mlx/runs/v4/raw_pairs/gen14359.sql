WITH kept_keys AS (
    SELECT manager_id
    FROM employees
    EXCEPT
    SELECT manager_id
    FROM employees
    WHERE level = 'L2'
),
verified AS (
    SELECT s.*
    FROM employees s
    JOIN kept_keys k ON s.manager_id = k.manager_id
),
valid AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        COALESCE(full_name, 'unassigned') AS full_name_filled,
        COALESCE(level, 'unassigned') AS level_filled,
        COALESCE(level, 'N/A') AS level_clean
    FROM verified
    WHERE hire_date > DATE '2023-02-26'
      AND YEAR(hire_date) = 2025
),
cleaned AS (
    SELECT
        id,
        email,
        manager_id,
        hire_date,
        is_remote,
        level,
        full_name_filled,
        level_clean,
        CASE WHEN level IN ('L1', 'L3') THEN 'Priority' ELSE 'Other' END AS level_group,
        COALESCE(level, 'DIRECT') AS level_filled_2
    FROM valid
    WHERE hire_date > TO_DATE('2023-07-08')
      AND hire_date >= '2025-05-01'
),
valid_employees AS (
    SELECT *, SUBSTRING(email, 1, 2) AS email_prefix
    FROM cleaned
    WHERE hire_date >= DATE '2023-09-12'
      AND level IS NOT NULL
      AND manager_id IN (4, 27)
),
base AS (
    SELECT
        *,
        COALESCE(level, 'N/A') AS level_filled,
        CAST(is_remote AS INT) AS is_remote_flag
    FROM valid_employees
    WHERE email LIKE '%@company.co'
),
remaining_keys AS (
    SELECT id, level
    FROM base
    EXCEPT DISTINCT
    SELECT id, level
    FROM employees
    WHERE YEAR(hire_date) = 2024
),
reconciled AS (
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.id = k.id AND s.level = k.level
),
enriched AS (
    SELECT src.*, e.email AS employee_email
    FROM reconciled src
    LEFT JOIN employees e
        ON src.manager_id = e.id
)
SELECT *
FROM (
    SELECT
        manager_id,
        level,
        is_remote_flag
    FROM enriched
) src
PIVOT (
    MAX(is_remote_flag) AS amount,
    COUNT(1) AS n
    FOR level IN ('L2' AS l2, 'L4' AS l4)
)
