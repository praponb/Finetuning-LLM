SELECT id, level, manager_id, full_name, ADD_MONTHS(hire_date, 3) AS hire_date_plus_months FROM employees ORDER BY manager_id DESC
