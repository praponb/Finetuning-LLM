select products.brand, date_trunc('MONTH', products.launch_date) launch_month, avg(order_items.unit_price) avg_unit_price
from order_items
left join orders on orders.id = order_items.order_id
inner join products on order_items.product_id = products.id
group by products.brand, date_trunc('MONTH', products.launch_date)
