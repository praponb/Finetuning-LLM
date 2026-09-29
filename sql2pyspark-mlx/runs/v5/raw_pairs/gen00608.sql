SELECT location, ROUND(STDDEV(budget), 2) AS stddev_budget, COUNT(*) AS cnt
FROM departments
WHERE budget >= 2354000 AND dept_name = 'HR'
GROUP BY location;
