select signup_date, 'customer' record_type
from customers
union
select opened_date, 'store' record_type
from stores
order by record_type desc
