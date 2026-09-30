select unit_price
from order_items
where order_id in (12, 25, 26)
intersect
select amount
from orders
where amount < 1290
