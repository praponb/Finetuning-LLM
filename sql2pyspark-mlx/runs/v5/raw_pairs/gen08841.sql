select hire_date, 'employee' origin
from employees
union all
select end_date hire_date, 'subscription' origin
from subscriptions
where customer_id is not null;
