select s.id, s.status
from subscriptions s
where s.customer_id not in (select id from customers where name is null)
