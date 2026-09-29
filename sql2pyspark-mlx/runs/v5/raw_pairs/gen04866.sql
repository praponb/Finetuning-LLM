select subscriptions.id, subscriptions.monthly_fee, subscriptions.status
from subscriptions
where subscriptions.customer_id in (select id from customers where month(signup_date) in (2, 7, 8))
