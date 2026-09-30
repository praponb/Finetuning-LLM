select country
from customers
union all
select country
from sessions
where customer_id is not null
