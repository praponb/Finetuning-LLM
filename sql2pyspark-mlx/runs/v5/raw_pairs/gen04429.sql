select
    case when departments.budget >= 3901000 then 'high' when departments.budget >= 1908000 then 'mid' else 'low' end as budget_bucket,
    departments.dept_name,
    sum(case when employees.level = 'L2' then 1 else 0 end) as l2_count,
    max(employees.salary) as max_salary
from employees
left join departments on employees.dept_id = departments.id
where departments.location = 'Bangkok'
group by case when departments.budget >= 3901000 then 'high' when departments.budget >= 1908000 then 'mid' else 'low' end, departments.dept_name
having max(employees.salary) > 58000;
