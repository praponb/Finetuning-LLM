select status
from orders
except
select status
from subscriptions;
