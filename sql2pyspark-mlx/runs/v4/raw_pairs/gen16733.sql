with DepartmentTotals as (
    select dept_id, sort_array(collect_set(level)) as level_list, avg(salary) as avg_salary
    from employees
    where manager_id is null
    group by dept_id
)
select
    d.id,
    d.location,
    D.level_list,
    D.avg_salary
from departments d
inner join DepartmentTotals D on d.id = D.dept_id
where D.avg_salary >= 96000
