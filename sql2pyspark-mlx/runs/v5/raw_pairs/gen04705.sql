select id, order_id
from payments
where amount > 1480
except
select id, order_id
from payments
where order_id in (6, 36, 40);
