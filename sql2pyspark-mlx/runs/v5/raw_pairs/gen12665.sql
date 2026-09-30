WITH filtered AS (
    SELECT
        id,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        CASE WHEN level = 'L1' THEN 1 ELSE 0 END AS is_l1,
        COALESCE(level, 'UNKNOWN') AS level_filled
    FROM employees
    WHERE hire_date >= TO_DATE('2023-02-12')
      AND level IS NOT NULL
)
SELECT
    id,
    email,
    dept_id,
    salary,
    hire_date,
    is_remote,
    level,
    is_l1,
    ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary, id) AS event_seq,
    LAST_VALUE(level) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
    SUM(salary) OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
    LAG(is_l1, 1) OVER (PARTITION BY level ORDER BY is_l1, id) AS prev_is_l1
FROM filtered
