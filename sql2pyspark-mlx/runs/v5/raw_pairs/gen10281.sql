select end_date
from subscriptions
where plan = 'basic'
union
select end_date
from subscriptions
where plan = 'enterprise'
