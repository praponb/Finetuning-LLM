select
    product_id,
    round(avg(quantity), 2) as avg_quantity,
    count(distinct unit_price) as unique_products,
    sum(quantity) as total_quantity
from order_items
group by product_id
having avg_quantity >= 30
