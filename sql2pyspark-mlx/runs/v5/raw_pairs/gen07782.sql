select amount
from payments
where paid_at < '2025-08-01'
union
select unit_price
from order_items
where quantity = 3;
