select id, customer_id, count(*) over (partition by customer_id) as customer_id_count
from subscriptions;
