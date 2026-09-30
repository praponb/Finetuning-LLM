WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM payments
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY paid_at DESC NULLS LAST) = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    order_id,
    STDDEV_SAMP(amount) AS amount_stddev,
    SIZE(COLLECT_SET(status)) AS num_status,
    MAX(amount) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_max_amount
FROM latest
GROUP BY DATE_TRUNC('MONTH', paid_at), order_id
