SELECT
    *,
    NTILE(10) OVER (PARTITION BY channel ORDER BY shipping_fee, id) AS shipping_fee_bucket,
    ROUND(amount / SUM(amount) OVER (PARTITION BY store_id), 4) AS amount_share
FROM orders
