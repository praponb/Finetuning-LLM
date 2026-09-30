SELECT customer_id, CEIL(shipping_fee) AS shipping_fee_whole
FROM orders
WHERE store_id = 18 AND discount <= 0.38 AND YEAR(order_date) = 2025;
