WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        status,
        channel,
        amount,
        shipping_fee,
        LOWER(channel) AS channel_lower,
        DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date
    FROM orders
    WHERE status IN ('CANCELLED', 'SHIPPED', 'RETURNED')
)
-- Attach stores attributes
SELECT
    s.*,
    p.city,
    p.region,
    p.opened_date
FROM valid s
LEFT JOIN stores p
    ON s.store_id = p.id;
