SELECT
    location,
    CASE WHEN budget >= 4150000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    SUM(CASE WHEN location = 'London' THEN budget ELSE 0 END) AS london_budget,
    ROUND(SUM(budget) / NULLIF(COUNT(CASE WHEN location = 'Singapore' THEN 1 END), 0), 2) AS budget_per_singapore,
    COUNT(DISTINCT dept_name) AS distinct_dept_names,
    COUNT(*) FILTER (WHERE dept_name = 'Sales') AS sales_rows
FROM departments
GROUP BY location, CASE WHEN budget >= 4150000 THEN 'PREMIUM' ELSE 'STANDARD' END
