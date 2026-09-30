select
    id,
    unit_price,
    product_id,
    ceil(unit_price) as unit_price_whole
from order_items;
