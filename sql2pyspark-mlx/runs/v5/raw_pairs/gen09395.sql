WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        CASE WHEN level = 'L5' THEN 1 ELSE 0 END AS is_l5
    FROM employees
    WHERE LENGTH(full_name) < 10
)
-- Running and moving aggregates
SELECT
    id,
    email,
    dept_id,
    manager_id,
    level,
    is_l5,
    LEAD(is_l5) OVER w_grp AS next_is_l5,
    LEAD(salary) OVER w_grp AS next_salary,
    ROW_NUMBER() OVER w_grp AS seq_no,
    LEAD(is_l5, 1) OVER (PARTITION BY level, manager_id ORDER BY hire_date, id) AS next_is_l5_2
FROM base
WINDOW w_grp AS (PARTITION BY dept_id, level ORDER BY is_l5, id)
