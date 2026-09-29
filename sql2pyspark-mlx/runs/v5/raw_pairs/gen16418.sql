SELECT *, COALESCE(budget, 0) AS budget_filled, UPPER(location) AS location_upper FROM departments WHERE dept_name = 'Engineering' AND dept_name IN ('HR', 'Sales');
