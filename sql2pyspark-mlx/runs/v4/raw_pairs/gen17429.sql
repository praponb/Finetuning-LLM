SELECT *, CONCAT_WS('/', dept_name, location) AS dept_name_location_key, ROUND(budget * 0.85, 2) AS budget_adjusted FROM departments WHERE dept_name <> 'Sales'
