SELECT
    CASE WHEN budget >= 3990000 THEN 'high' WHEN budget >= 846000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    CASE WHEN dept_name IN ('Sales', 'Legal') THEN 'group_a' ELSE 'other' END AS dept_name_group,
    ROUND((100 * SUM(CASE WHEN dept_name = 'Legal' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_legal,
    SUM(budget) AS total_budget
FROM departments
GROUP BY CASE WHEN budget >= 3990000 THEN 'high' WHEN budget >= 846000 THEN 'mid' ELSE 'low' END, CASE WHEN dept_name IN ('Sales', 'Legal') THEN 'group_a' ELSE 'other' END
HAVING SUM(CASE WHEN dept_name = 'Finance' THEN 1 ELSE 0 END) < 49
ORDER BY dept_name_group
