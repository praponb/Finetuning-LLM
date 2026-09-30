SELECT customer_id, shipping_fee + amount AS amount_with_shipping
FROM orders
ORDER BY 2 DESC;
