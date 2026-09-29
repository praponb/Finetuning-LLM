SELECT id, budget, dept_name, location, CASE WHEN location = 'Chiang Mai' THEN 1 ELSE 0 END AS is_chiang_mai FROM departments;
