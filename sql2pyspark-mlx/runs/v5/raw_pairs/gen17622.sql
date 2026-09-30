select max(unit_price) as max_unit_price, max(quantity) - min(quantity) as quantity_range
from order_items
