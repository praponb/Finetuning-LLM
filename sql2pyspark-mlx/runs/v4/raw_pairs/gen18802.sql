SELECT *, CONCAT(full_name, ' - ', level) AS full_name_level, LAST_DAY(hire_date) AS hire_date_month_end FROM employees WHERE level NOT IN ('L4', 'L5') OR job_title IN ('Director', 'Engineer')
