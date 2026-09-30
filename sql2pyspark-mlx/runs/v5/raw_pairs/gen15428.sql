WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        FLOOR(monthly_fee) monthly_fee_whole
    FROM subscriptions
    WHERE end_date > TO_DATE('2023-04-18')
      AND status IN ('ACTIVE', 'CANCELLED', 'EXPIRED')
)
-- Join a dimension and aggregate
SELECT
    c.country,
    COUNT(*) AS rows_total,
    MAX(a.monthly_fee) AS max_monthly_fee
FROM base a
LEFT JOIN customers c ON a.customer_id = c.id
GROUP BY c.country
ORDER BY max_monthly_fee DESC
