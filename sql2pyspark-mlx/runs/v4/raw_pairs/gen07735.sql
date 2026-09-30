WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        status,
        ROUND(amount, 2) AS amount_rounded
    FROM payments
    WHERE order_id IN (7, 12, 13, 28)
),
base_payments AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(method) AS method_length,
        COALESCE(method, 'N/A') AS method_filled
    FROM base
    WHERE method IS NOT NULL
      AND method IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT s.*, o.amount
    FROM base_payments s
    INNER JOIN orders o
        ON s.order_id = o.id
)
-- Grouped metrics
SELECT
    CASE WHEN method_length >= 62 THEN 'PREMIUM' ELSE 'STANDARD' END AS method_length_tier,
    order_id,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(*) AS event_count,
    ROUND(AVG(amount_rounded), 2) AS avg_amount_rounded,
    APPROX_PERCENTILE(amount, 0.5) AS amount_p50
FROM enriched
GROUP BY method_length_tier, order_id
ORDER BY avg_amount_rounded
