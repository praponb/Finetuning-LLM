SELECT
    dept_name,
    PERCENTILE_APPROX(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    SUM(CASE WHEN location = 'London' THEN budget ELSE 0 END) AS london_budget,
    SUM(CASE WHEN location = 'Berlin' THEN budget ELSE 0 END) AS berlin_budget,
    ROUND(AVG(budget), 2) AS avg_budget,
    COUNT_IF(location = 'Berlin') AS berlin_events
FROM departments
GROUP BY 1
ORDER BY london_budget DESC NULLS FIRST;
