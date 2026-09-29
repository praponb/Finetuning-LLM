select status
from orders
where store_id = 12
union all
select status
from payments
where order_id in (11, 31, 33, 36)
order by status desc;
