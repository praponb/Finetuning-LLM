WITH base AS (
    SELECT *
    FROM employees
    WHERE is_remote = TRUE AND dept_id IN (15, 17, 38, 40) AND manager_id IN (4, 12, 33)
),
summary AS (
    SELECT
        level,
        manager_id,
        ROUND((100 * SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l5,
        ROUND(STDDEV(salary), 2) AS stddev_salary
    FROM base
    GROUP BY level, manager_id
)
SELECT
    level,
    manager_id,
    pct_l5,
    stddev_salary
FROM summary
WHERE pct_l5 > 53;
