SELECT
    id,
    customer_id,
    amount,
    store_id
FROM orders
WHERE store_id IS NULL;
