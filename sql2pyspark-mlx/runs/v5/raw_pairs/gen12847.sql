SELECT
    id,
    channel,
    customer_id,
    amount
FROM orders
WHERE amount < 630
ORDER BY id DESC
