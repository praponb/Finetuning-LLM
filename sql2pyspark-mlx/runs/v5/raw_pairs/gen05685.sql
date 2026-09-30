select id, customer_id, round(shipping_fee * 1.1, 2) as shipping_fee_scaled from orders where store_id is null order by shipping_fee_scaled desc, id limit 5;
