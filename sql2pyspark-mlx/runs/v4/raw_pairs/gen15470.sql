select hire_date, level from employees where job_title in ('Manager', 'Director', 'Engineer') except select hire_date, level from employees where full_name like '%Chai%'
