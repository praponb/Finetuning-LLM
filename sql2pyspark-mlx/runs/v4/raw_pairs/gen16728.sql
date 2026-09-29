WITH with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.quantity
    FROM orders s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    DATE_TRUNC('WEEK', order_date) AS week_start,
    COUNT(*) AS cnt,
    CORR(discount, shipping_fee) AS corr_discount_shipping_fee,
    ROUND(SUM(shipping_fee) / NULLIF(COUNT(CASE WHEN channel = 'web' THEN 1 END), 0), 2) AS shipping_fee_per_web,
    SUM(CASE WHEN status = 'COMPLETED' THEN shipping_fee ELSE 0 END) AS completed_shipping_fee,
    MAX(shipping_fee) AS max_shipping_fee,
    COUNT_IF(status = 'COMPLETED') AS completed_events
FROM with_last
GROUP BY ROLLUP (DATE_TRUNC('MONTH', order_date), DATE_TRUNC('WEEK', order_date))
ORDER BY cnt DESC;
