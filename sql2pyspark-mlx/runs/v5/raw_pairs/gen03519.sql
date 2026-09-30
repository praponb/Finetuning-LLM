WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(amount * 1.07, 2) AS amount_adjusted,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM orders
    WHERE order_date > DATE '2023-09-12'
)
-- Grouped metrics
SELECT
    CASE WHEN discount >= 0.38 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_tier,
    COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    MIN(discount) AS min_discount,
    COUNT(*) AS event_count,
    AVG(amount) FILTER (WHERE status = 'PENDING') AS pending_avg_amount
FROM cleaned
GROUP BY discount_tier
