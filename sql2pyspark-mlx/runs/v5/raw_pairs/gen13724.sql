select case when salary >= 164000 then 'high' when salary >= 42000 then 'mid' else 'low' end salary_bucket, sum(salary) total_salary
from employees
group by 1
having total_salary >= 550000;
