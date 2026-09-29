select opened_date as event_date
from accounts
except
select end_date as event_date
from subscriptions
where plan <> 'premium'
