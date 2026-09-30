with ranked_employees as (
    select
        id,
        level,
        hire_date,
        email,
        dense_rank() over (partition by level order by hire_date desc) as rnk
    from employees
)
select
    id,
    level,
    hire_date,
    email
from ranked_employees
where rnk <= 5
order by hire_date;
