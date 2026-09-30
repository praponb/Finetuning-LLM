select status
from accounts
where status in ('CLOSED', 'OPEN')
intersect
select status
from subscriptions
where status = 'EXPIRED'
order by status;
