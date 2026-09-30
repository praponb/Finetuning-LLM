select unit_price
from order_items
union
select amount
from payments;
