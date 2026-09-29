select opened_date
from accounts
where status = 'OPEN'
union
select signup_date
from customers;
