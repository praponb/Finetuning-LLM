select order_id, product_id, max(quantity) as max_quantity
from order_items
where product_id = 30 or quantity between 3 and 7
group by rollup(order_id, product_id)
order by product_id asc, order_id asc
