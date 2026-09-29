WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        status,
        LAG(amount) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) AS prev_amount,
        LEAD(amount) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) AS event_seq,
        SUM(amount) OVER (PARTITION BY status ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM payments
    WHERE order_id = 33
)
SELECT
    id,
    order_id,
    status,
    prev_amount,
    next_amount,
    event_seq,
    cumulative_amount,
    CASE WHEN status RLIKE '^(REFUNDED|SUCCESS)$' THEN 'OTHER_ZONE' WHEN status RLIKE '^(FAILED)$' THEN 'CORE' ELSE 'EXTENDED' END AS status_zone
FROM enriched
ORDER BY next_amount DESC NULLS FIRST
