with numbered as (
    select *, row_number() over (partition by dept_id order by salary desc, id) as rnk
    from employees
    where dept_id = 2
)
select *
from numbered
where rnk <= 5
order by dept_id;
