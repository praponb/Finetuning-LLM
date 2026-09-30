select s.id, s.start_date, s.customer_id
from subscriptions s
where s.customer_id in (select id from customers where age between 47 and 57)
