select
    id,
    unit_price,
    quantity,
    product_id
from order_items
where unit_price >= (select avg(unit_price) from order_items);
