select *, discount * shipping_fee as discount_times_shipping_fee from orders where order_date >= '2024-05-01' and store_id is not null order by customer_id, id limit 20;
