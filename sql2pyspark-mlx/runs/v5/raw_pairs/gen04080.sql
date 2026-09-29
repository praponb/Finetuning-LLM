select
    id,
    brand,
    price,
    array_join(tags, ', ') tags_text,
    greatest(cost, price) greatest_cost_price
from products
order by id desc;
