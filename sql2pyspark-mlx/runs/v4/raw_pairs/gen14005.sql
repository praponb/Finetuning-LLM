SELECT id, full_name, salary, hire_date, LAST_DAY(hire_date) AS hire_date_month_end FROM employees WHERE level = 'L5' ORDER BY salary DESC, hire_date_month_end ASC, id ASC LIMIT 10
