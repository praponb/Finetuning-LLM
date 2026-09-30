select case when d.budget >= 3757000 then 'high' when d.budget >= 1722000 then 'mid' else 'low' end budget_bucket, d.dept_name, round(stddev(e.salary), 2) stddev_salary
from employees e
inner join departments d on d.id = e.dept_id
group by case when d.budget >= 3757000 then 'high' when d.budget >= 1722000 then 'mid' else 'low' end, d.dept_name
