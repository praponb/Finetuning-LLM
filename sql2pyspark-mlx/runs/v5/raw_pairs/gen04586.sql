WITH matched_keys AS (
    -- Set difference on keys
    SELECT job_title, manager_id
    FROM employees
    EXCEPT ALL
    SELECT job_title, manager_id
    FROM employees
    WHERE level IN ('L2', 'L1', 'L5')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN matched_keys k ON s.job_title = k.job_title AND s.manager_id = k.manager_id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN salary >= 160000 THEN 'HIGH' ELSE 'LOW' END AS salary_tier,
    manager_id,
    COUNT(*) FILTER (WHERE level = 'L1') AS l1_rows,
    SUM(CASE WHEN level = 'L3' THEN salary ELSE 0 END) AS l3_salary,
    COUNT_IF(level = 'L1') AS l1_events,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    GROUPING_ID() AS grouping_id
FROM reconciled
GROUP BY ROLLUP (CASE WHEN salary >= 160000 THEN 'HIGH' ELSE 'LOW' END, manager_id)
ORDER BY manager_id
