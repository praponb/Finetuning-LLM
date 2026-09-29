select s.id, s.status
from subscriptions s
where s.plan in ('basic', 'standard') and s.customer_id in (select id from customers where age > 64)
order by id;
