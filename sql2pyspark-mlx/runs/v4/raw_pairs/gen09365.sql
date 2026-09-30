with ranked_employees as (
    select
        id,
        manager_id,
        hire_date,
        is_remote,
        rank() over (partition by manager_id order by hire_date desc) as rnk
    from employees
    where is_remote = true
)
select
    id,
    manager_id,
    hire_date,
    is_remote
from ranked_employees
where rnk <= 1
