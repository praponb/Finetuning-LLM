select opened_date
from accounts
union
select end_date as opened_date
from subscriptions;
