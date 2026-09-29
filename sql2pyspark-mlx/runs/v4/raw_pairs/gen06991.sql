SELECT
    location,
    SUM(CASE WHEN location = 'Bangkok' THEN budget ELSE 0 END) AS bangkok_budget,
    MAX(budget) AS max_budget,
    SUM(CASE WHEN location = 'London' THEN budget ELSE 0 END) AS london_budget,
    MIN(budget) AS min_budget
FROM departments
GROUP BY location
ORDER BY location DESC
