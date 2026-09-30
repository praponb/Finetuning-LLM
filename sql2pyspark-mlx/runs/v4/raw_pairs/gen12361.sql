SELECT
    budget_bucket,
    location,
    stddev_budget,
    unique_dept_names
FROM (
    SELECT
        CASE WHEN budget >= 4399000 THEN 'high' WHEN budget >= 1933000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        location,
        ROUND(STDDEV(budget), 2) AS stddev_budget,
        COUNT(DISTINCT budget) AS unique_dept_names
    FROM departments
    GROUP BY CASE WHEN budget >= 4399000 THEN 'high' WHEN budget >= 1933000 THEN 'mid' ELSE 'low' END, location
) AS agg
WHERE unique_dept_names < 5
ORDER BY stddev_budget;
