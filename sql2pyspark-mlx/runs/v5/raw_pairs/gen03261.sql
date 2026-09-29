SELECT dept_name, stddev_budget, pct_sales
FROM (
    SELECT dept_name, ROUND(STDDEV(budget), 2) AS stddev_budget, ROUND((100 * SUM(CASE WHEN dept_name = 'Sales' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_sales
    FROM departments
    GROUP BY dept_name
) AS t
WHERE pct_sales >= 30;
