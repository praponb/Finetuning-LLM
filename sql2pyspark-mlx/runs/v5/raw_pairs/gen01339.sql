WITH with_last AS (
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.channel,
        s.discount,
        latest.unit_price
    FROM orders s
    LEFT JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    CASE WHEN discount >= 0.41 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_tier,
    CASE WHEN unit_price >= 450 THEN 'HIGH' ELSE 'LOW' END AS unit_price_tier,
    SUM(unit_price) FILTER (WHERE channel = 'mobile') AS mobile_total_unit_price,
    COUNT(*) AS event_count,
    COUNT_IF(channel = 'mobile') AS mobile_events,
    COUNT(*) FILTER (WHERE channel = 'web') AS web_rows,
    SUM(unit_price) AS total_unit_price
FROM with_last
GROUP BY CASE WHEN discount >= 0.41 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN unit_price >= 450 THEN 'HIGH' ELSE 'LOW' END
ORDER BY total_unit_price;
