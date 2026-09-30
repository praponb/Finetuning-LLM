select opened_date
from accounts
union
select start_date opened_date
from subscriptions
order by opened_date desc;
