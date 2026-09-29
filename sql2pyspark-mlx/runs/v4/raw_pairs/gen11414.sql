select date_trunc('MONTH', customers.signup_date) as signup_month, max(orders.shipping_fee) - min(orders.shipping_fee) as shipping_fee_range, max(orders.order_date) as last_order_date
from orders
left join stores on orders.store_id = stores.id
left join customers on orders.customer_id = customers.id
group by date_trunc('MONTH', customers.signup_date)
having max(orders.shipping_fee) - min(orders.shipping_fee) > 42
order by signup_month;
