WITH matched_keys AS (
    -- Set difference on keys
    SELECT level, id
    FROM employees
    EXCEPT
    SELECT level, id
    FROM employees
    WHERE email IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN matched_keys k ON s.level = k.level AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN salary >= 128000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
    COUNT(*) FILTER (WHERE job_title = 'Designer') AS designer_rows,
    SUM(CASE WHEN job_title = 'Engineer' THEN salary ELSE 0 END) AS engineer_salary,
    STDDEV_SAMP(salary) AS salary_stddev
FROM kept_rows
GROUP BY CASE WHEN salary >= 128000 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(*) FILTER (WHERE job_title = 'Designer') >= 1
ORDER BY salary_tier
