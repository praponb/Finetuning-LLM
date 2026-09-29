SELECT *, SUBSTRING(location, 1, 3) AS location_prefix, COALESCE(budget, 0) AS budget_filled
FROM departments
WHERE budget IS NULL AND location IN ('Bangkok', 'Singapore')
