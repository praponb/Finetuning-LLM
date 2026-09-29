SELECT
    id,
    customer_id,
    discount,
    store_id,
    AVG(discount) OVER (PARTITION BY customer_id) AS customer_id_avg_discount,
    AVG(discount) OVER (PARTITION BY store_id, customer_id) AS store_id_avg_discount
FROM orders;
