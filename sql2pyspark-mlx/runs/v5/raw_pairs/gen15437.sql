select c.id, c.is_active, c.country
from customers c
where c.id in (select customer_id from orders)
order by c.country desc
