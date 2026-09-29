WITH filtered_employees AS (
    SELECT *, CASE WHEN level = 'L5' THEN 1 ELSE 0 END AS is_l5
    FROM employees
    WHERE NOT (salary >= 120000 AND level NOT IN ('L2', 'L4'))
),
summary AS (
    SELECT is_remote, ROUND((100 * SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l4
    FROM filtered_employees
    GROUP BY is_remote
)
SELECT *
FROM summary
WHERE pct_l4 >= 67
ORDER BY pct_l4 DESC;
