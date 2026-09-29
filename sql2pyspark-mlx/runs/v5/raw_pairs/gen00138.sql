select *, quantity - unit_price as line_total, unit_price - quantity as line_total_2
from order_items
where order_id in (11, 14);
