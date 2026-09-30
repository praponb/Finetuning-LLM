WITH valid AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date
    FROM orders
    WHERE order_date >= DATE '2023-10-28'
),
eligible AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.unit_price > 260
        )
),
with_last AS (
    SELECT s.*, latest.order_item_unit_price
    FROM eligible s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
filtered AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        days_since_order_date,
        CAST(discount AS INT) AS discount_int,
        discount - days_since_order_date AS discount_minus_days_since_order_date,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM with_last
    WHERE order_date > DATE '2024-04-21'
)
SELECT
    DATE_TRUNC('WEEK', order_date) AS week_start,
    COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count,
    STDDEV_SAMP(discount) AS discount_stddev,
    SUM(CASE WHEN status = 'RETURNED' THEN amount ELSE 0 END) AS returned_amount,
    COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed_rows
FROM filtered
GROUP BY week_start
HAVING discount_stddev < 0
ORDER BY completed_rows DESC NULLS LAST;
