select
    id,
    order_id,
    unit_price,
    floor(quantity) as quantity_whole
from order_items;
