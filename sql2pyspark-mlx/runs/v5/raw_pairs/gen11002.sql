WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        CONCAT_WS(',', status, method) AS status_method_key
    FROM payments
    WHERE order_id IS NOT NULL
)
-- Running and moving aggregates
SELECT
    id,
    method,
    amount,
    paid_at,
    status_method_key,
    SUM(amount) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY order_id), 4) AS amount_share,
    ROW_NUMBER() OVER (PARTITION BY method, status ORDER BY amount, id) AS event_seq
FROM cleaned;
