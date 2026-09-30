SELECT MAX(budget) - MIN(budget) AS budget_range, SUM(CASE WHEN dept_name = 'Legal' THEN 1 ELSE 0 END) AS legal_count, SUM(CASE WHEN dept_name = 'HR' THEN 1 ELSE 0 END) AS hr_count
FROM departments
WHERE location IN ('London', 'Chiang Mai', 'Berlin') AND dept_name IN ('Legal', 'Sales', 'Finance')
