select 'subscription' as record_type, start_date
from subscriptions
where year(end_date) = 2023
union
select 'order' as record_type, order_date
from orders
where discount <= 0.36
order by start_date;
