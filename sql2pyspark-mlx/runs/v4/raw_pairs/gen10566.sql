select s.id, s.browser
from sessions s
where s.customer_id is not null and s.customer_id in (select id from customers where credit_limit > 11800)
