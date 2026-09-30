select * from (select level, job_title, salary from employees) pivot (sum(salary) for job_title in ('Analyst', 'Designer', 'Engineer'));
