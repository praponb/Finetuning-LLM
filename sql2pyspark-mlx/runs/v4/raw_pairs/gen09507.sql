SELECT
    id,
    store_id,
    channel,
    status
FROM orders
WHERE store_id IN (2, 14, 20) AND amount <= 1130
ORDER BY channel
