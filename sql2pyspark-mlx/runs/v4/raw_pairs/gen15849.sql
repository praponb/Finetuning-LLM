select order_id, max(unit_price) - min(unit_price) as unit_price_range
from order_items
group by order_id
order by unit_price_range desc
