select * from (select level, job_title, salary from employees)
pivot (max(salary) for job_title in ('Manager', 'Analyst', 'Engineer'))
