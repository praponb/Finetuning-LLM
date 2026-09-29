with department_summary as (
    select dept_id, round(stddev(salary), 2) stddev_salary
    from employees
    group by dept_id
)
select
    d.id,
    d.budget,
    d.location,
    ds.stddev_salary
from departments d
inner join department_summary ds on d.id = ds.dept_id
where ds.stddev_salary > 51
order by id
limit 50
