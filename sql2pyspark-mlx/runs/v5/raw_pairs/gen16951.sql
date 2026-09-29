with department_stats as (
    select dept_id, sum(salary) as total_salary
    from employees
    group by dept_id
)
select d.dept_name, ds.total_salary
from departments d
left outer join department_stats ds on d.id = ds.dept_id
where ds.total_salary > 551000;
