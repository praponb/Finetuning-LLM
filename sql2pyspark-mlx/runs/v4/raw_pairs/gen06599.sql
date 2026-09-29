SELECT dept_name_group, budget_range
FROM (
    SELECT CASE WHEN dept_name IN ('Legal', 'Sales') THEN 'core' ELSE 'other' END AS dept_name_group, MAX(budget) - MIN(budget) AS budget_range
    FROM departments
    WHERE location != 'Bangkok'
    GROUP BY CASE WHEN dept_name IN ('Legal', 'Sales') THEN 'core' ELSE 'other' END
) AS sub
WHERE budget_range < 33
ORDER BY dept_name_group DESC
