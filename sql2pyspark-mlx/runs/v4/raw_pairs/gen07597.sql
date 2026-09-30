select job_title, min(hire_date) as first_hire_date, avg(salary) as avg_salary from employees group by 1 order by job_title
