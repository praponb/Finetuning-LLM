SELECT
    *,
    COUNT(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_count_30d,
    PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) AS amount_percent_rank,
    LAG(amount, 2) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_amount,
    AVG(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior
FROM payments
WHERE HOUR(paid_at) BETWEEN 8 AND 12;
