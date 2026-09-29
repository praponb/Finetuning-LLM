select opened_date
from accounts
where customer_id is not null
union
select signup_date
from customers
order by opened_date
