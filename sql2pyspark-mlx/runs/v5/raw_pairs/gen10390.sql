WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.salary BETWEEN 71000 AND 127000
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN budget >= 4219000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    STDDEV_SAMP(budget) AS budget_stddev,
    COUNT(*) FILTER (WHERE location = 'Berlin') AS berlin_rows,
    STDDEV_SAMP(budget) AS budget_stddev_2
FROM not_flagged
GROUP BY 1
ORDER BY berlin_rows DESC
