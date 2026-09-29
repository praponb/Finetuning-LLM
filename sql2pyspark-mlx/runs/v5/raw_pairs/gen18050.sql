SELECT
    id,
    customer_id,
    store_id,
    order_date
FROM orders
WHERE amount >= 730
ORDER BY customer_id DESC;
