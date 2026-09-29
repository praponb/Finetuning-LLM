select status
from accounts
where account_type = 'CREDIT'
except
select status
from subscriptions
where customer_id is null
order by status
