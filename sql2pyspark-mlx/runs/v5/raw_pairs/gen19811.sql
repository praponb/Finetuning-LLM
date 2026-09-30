WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount) OVER w_recent AS amount_avg_5,
        RANK() OVER w_part AS amount_rank
    FROM payments
    WINDOW w_recent AS (ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY status ORDER BY amount DESC)
)
SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    amount_avg_5,
    amount_rank,
    CASE WHEN method RLIKE '^(cash|paypal)$' THEN 'CORE' ELSE 'EXTENDED' END AS method_zone
FROM metrics
ORDER BY method, order_id DESC
