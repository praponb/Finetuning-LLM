select
    id,
    location,
    dept_name,
    budget,
    case when dept_name = 'HR' then 1 else 0 end as is_hr
from departments
where budget between 2298000 and 3296000
order by 4 asc, dept_name asc, id asc
limit 10;
