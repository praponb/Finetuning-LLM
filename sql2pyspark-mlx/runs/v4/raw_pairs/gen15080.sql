with numbered as (
    select *, dense_rank() over (partition by manager_id order by salary) as row_num
    from employees
)
select *
from numbered
where row_num <= 5
order by email asc;
