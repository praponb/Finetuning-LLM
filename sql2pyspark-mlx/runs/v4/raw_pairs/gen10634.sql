select level, email
from employees
where email like '%@startup.io'
union
select level, email
from employees
where manager_id = 31
order by level desc;
