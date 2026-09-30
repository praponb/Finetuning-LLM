select
    order_id,
    product_id,
    least(quantity, unit_price) least_quantity_unit_price,
    floor(quantity) quantity_whole
from order_items;
