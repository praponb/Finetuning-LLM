select is_remote, min(salary) as min_salary, avg(salary) as avg_salary, count(case when job_title = 'Analyst' then 1 end) as analyst_count from employees group by is_remote order by min_salary;
