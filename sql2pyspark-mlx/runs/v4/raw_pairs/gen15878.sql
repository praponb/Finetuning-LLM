select *, row_number() over (partition by order_id order by quantity desc nulls last, id) as rank_in_group
from order_items
qualify rank_in_group = 1;
