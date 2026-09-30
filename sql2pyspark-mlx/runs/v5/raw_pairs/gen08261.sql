select hire_date
from employees
union all
select end_date as hire_date
from subscriptions;
