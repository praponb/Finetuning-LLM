select status
from payments
intersect
select status
from subscriptions
order by status
