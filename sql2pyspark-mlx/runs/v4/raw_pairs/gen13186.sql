select id, amount, method
from payments
where amount > (select avg(amount) from payments where order_id in (21, 30, 35))
