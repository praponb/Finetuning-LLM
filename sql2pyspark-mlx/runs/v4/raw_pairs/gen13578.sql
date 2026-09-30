with department_stats as (
    select dept_id, count(distinct salary) as unique_levels
    from employees
    group by dept_id
)
select departments.id, departments.dept_name, department_stats.unique_levels
from departments
join department_stats on departments.id = department_stats.dept_id
order by department_stats.unique_levels desc, id;
