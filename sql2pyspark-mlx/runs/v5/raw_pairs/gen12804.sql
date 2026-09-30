select
    id,
    order_id,
    unit_price,
    row_number() over (partition by product_id, order_id order by quantity desc, id) as event_seq,
    cume_dist() over (order by unit_price nulls last) as unit_price_cume_dist,
    sum(quantity) over (partition by product_id order by unit_price, id rows between unbounded preceding and current row) as cumulative_quantity
from order_items;
