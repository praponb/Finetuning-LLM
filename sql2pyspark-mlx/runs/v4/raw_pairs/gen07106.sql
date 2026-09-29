SELECT
    id,
    budget,
    location,
    COALESCE(location, 'unassigned') AS location_filled,
    CASE WHEN budget >= 3845000 THEN 'High' WHEN budget >= 1895000 THEN 'Medium' ELSE 'Low' END AS budget_level
FROM departments
WHERE budget <= 1585000
ORDER BY budget
