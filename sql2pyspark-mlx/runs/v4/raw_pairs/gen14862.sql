select id, job_title, hire_date, salary, lag(salary, 1) over (partition by job_title order by hire_date, id) as prev_salary from employees where lower(email) like '%avi%'
