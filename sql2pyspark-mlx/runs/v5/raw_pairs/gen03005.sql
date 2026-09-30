SELECT
    id,
    channel,
    store_id,
    amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY channel, store_id), 3) AS amount_share
FROM orders
ORDER BY id DESC;
