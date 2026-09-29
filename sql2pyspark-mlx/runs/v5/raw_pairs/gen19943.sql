select status, 'account' as record_type
from accounts
union
select status, 'subscription' as record_type
from subscriptions
