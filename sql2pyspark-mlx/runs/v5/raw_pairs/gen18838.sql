select end_date as event_date, 'subscription' as kind
from subscriptions
union
select launch_date as event_date, 'product' as kind
from products
where brand in ('Acme', 'Umbrella', 'Initech')
order by kind;
