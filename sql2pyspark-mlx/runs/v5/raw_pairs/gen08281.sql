select *
from (
    select *, rank() over (partition by level order by salary desc) as rn
    from employees
) t
where rn = 1
