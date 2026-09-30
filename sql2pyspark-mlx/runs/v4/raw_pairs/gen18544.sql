select start_date, 'subscription' as source
from subscriptions
union
select launch_date as start_date, 'product' as source
from products;
