select *, rank() over (partition by product_id order by unit_price desc) as rank_in_group
from order_items
qualify rank_in_group = 1
