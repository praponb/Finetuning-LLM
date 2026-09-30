select
    products.stock_qty,
    order_items.product_id,
    order_items.order_id,
    order_items.quantity,
    last_day(products.launch_date) as launch_date_month_end
from products
inner join order_items on products.id = order_items.product_id
order by product_id desc;
