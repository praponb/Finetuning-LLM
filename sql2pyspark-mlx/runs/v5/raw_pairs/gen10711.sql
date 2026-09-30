select status
from accounts
where customer_id is not null
union all
select status
from subscriptions;
