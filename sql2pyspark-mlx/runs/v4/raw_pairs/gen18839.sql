select id, email, dept_id, date_trunc('YEAR', hire_date) as hire_date_period from employees where upper(full_name) like '%MMA%' and manager_id = 14
