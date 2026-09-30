with department_stats as (
    select dept_id, avg(salary) as avg_salary
    from employees
    group by dept_id
)
select d.dept_name, ds.avg_salary
from departments d
left outer join department_stats ds on d.id = ds.dept_id
where ds.avg_salary > 137000
order by dept_name;
