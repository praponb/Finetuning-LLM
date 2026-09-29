WITH flagged AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
islands AS (
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    id,
    order_id,
    method,
    burst_id,
    CASE WHEN method RLIKE '^(paypal|card)$' THEN 'GROUP_B' WHEN method RLIKE '^(bank_transfer|cash)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS method_zone
FROM islands
ORDER BY method_zone DESC;
