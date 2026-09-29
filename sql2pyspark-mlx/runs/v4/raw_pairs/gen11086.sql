with base as (
    select
        id,
        full_name,
        dept_id,
        manager_id,
        salary,
        is_remote,
        level,
        upper(level) as level_upper
    from employees
    where hire_date > to_date('2023-10-08')
)
select
    *,
    min(salary) over (partition by manager_id) as manager_id_min_salary,
    sum(salary) over (order by salary asc, id asc rows between unbounded preceding and current row) as cumulative_salary,
    sum(salary) over (partition by level order by salary asc, id asc rows between unbounded preceding and current row) as running_salary
from base;
