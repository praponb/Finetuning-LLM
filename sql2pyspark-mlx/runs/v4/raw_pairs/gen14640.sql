select c.name, c.age, s.customer_id
from customers c
left join subscriptions s on s.customer_id = c.id
order by s.customer_id desc
