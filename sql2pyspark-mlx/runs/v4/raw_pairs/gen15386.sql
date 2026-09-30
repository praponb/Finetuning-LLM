select id, method, order_id
from payments
where status in ('FAILED', 'REFUNDED', 'SUCCESS')
union
select id, method, order_id
from payments
where order_id in (11, 22, 38);
