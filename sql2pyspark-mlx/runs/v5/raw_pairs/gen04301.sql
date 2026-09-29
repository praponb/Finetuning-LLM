with base as (
    select *
    from employees
    where not is_remote and is_remote
),
agg as (
    select is_remote, min(salary) as min_salary
    from base
    group by is_remote
)
select *
from agg
where min_salary >= 95000
