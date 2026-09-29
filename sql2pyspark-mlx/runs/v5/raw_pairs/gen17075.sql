with numbered as (
    select
        id,
        manager_id,
        hire_date,
        row_number() over (partition by manager_id order by hire_date, id) rnk
    from employees
    where job_title = 'Designer'
)
select id, manager_id, hire_date
from numbered
where rnk = 1
