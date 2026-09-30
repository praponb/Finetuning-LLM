select count(distinct quantity) as unique_products, min(quantity) as min_quantity
from order_items
order by unique_products
