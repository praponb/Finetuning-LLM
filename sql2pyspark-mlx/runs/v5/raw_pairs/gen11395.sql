SELECT store_id, MIN(order_date) AS first_order_date, MIN(discount) AS min_discount
FROM orders
GROUP BY store_id
ORDER BY first_order_date, store_id DESC;
