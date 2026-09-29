SELECT
    dept_name,
    location,
    CASE WHEN budget >= 3376000 THEN 'high' WHEN budget >= 1590000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    MIN(budget) AS min_budget,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM departments
WHERE budget BETWEEN 3218000 AND 3714000
GROUP BY 1, 2, 3
HAVING MIN(budget) >= 3417000
ORDER BY 3, location;
