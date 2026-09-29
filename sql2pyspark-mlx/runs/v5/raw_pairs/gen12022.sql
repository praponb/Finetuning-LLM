select products.brand, min(order_items.unit_price) as min_unit_price, min(order_items.quantity) as min_quantity
from order_items
left outer join products on order_items.product_id = products.id
join orders on orders.id = order_items.order_id
group by products.brand
order by brand desc, min_unit_price;
