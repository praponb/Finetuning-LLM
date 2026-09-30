WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    id,
    order_id,
    method,
    paid_at,
    session_id,
    CASE WHEN method RLIKE '^(bank_transfer|promptpay)$' THEN 'EXTENDED' WHEN method RLIKE '^(card|paypal)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS method_zone
FROM sessionized
ORDER BY paid_at DESC NULLS FIRST;
