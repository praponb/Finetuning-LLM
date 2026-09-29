with department_summary as (
    select dept_id, min(salary) as min_salary
    from employees
    group by dept_id
)
select
    d.id,
    d.budget,
    d.location,
    ds.min_salary
from departments d
join department_summary ds on d.id = ds.dept_id
where ds.min_salary >= 145000;
