SELECT id, location, TRIM(dept_name) AS clean_dept_name, CAST(budget AS INT) AS budget_int FROM departments WHERE budget >= 3052000 AND (location = 'London' OR location = 'Tokyo');
