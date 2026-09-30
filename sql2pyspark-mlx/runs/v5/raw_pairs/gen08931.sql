SELECT *, DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_date_label FROM employees WHERE level IN ('L3', 'L1', 'L4')
