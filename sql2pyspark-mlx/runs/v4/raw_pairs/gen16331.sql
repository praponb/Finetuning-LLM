with department_summary as (
    select dept_id, round(stddev(salary), 2) as stddev_salary, max(salary) as max_salary
    from employees
    group by dept_id
)
select
    d.id,
    d.dept_name,
    ds.stddev_salary,
    ds.max_salary
from departments d
left join department_summary ds on d.id = ds.dept_id
where ds.max_salary > 113000
order by ds.stddev_salary
