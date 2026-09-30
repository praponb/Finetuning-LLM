WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(amount) AS amount_whole,
        COALESCE(method, 'UNKNOWN') AS resolved_method
    FROM payments
    WHERE paid_at >= '2024-09-10'
)
-- Grouped metrics
SELECT
    CASE WHEN amount >= 1490 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    CASE WHEN amount_whole >= 63 THEN 'LARGE' ELSE 'SMALL' END AS amount_whole_tier,
    ROUND(SUM(amount_whole) / NULLIF(COUNT(CASE WHEN method = 'paypal' THEN 1 END), 0), 2) AS amount_whole_per_paypal,
    SUM(CASE WHEN method = 'promptpay' THEN amount ELSE 0 END) AS promptpay_amount,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    GROUPING_ID() AS gid
FROM filtered
GROUP BY GROUPING SETS ((CASE WHEN amount >= 1490 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN amount_whole >= 63 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN amount >= 1490 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
ORDER BY promptpay_amount ASC;
