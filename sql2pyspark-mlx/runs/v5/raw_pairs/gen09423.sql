WITH with_latest AS (
    SELECT
        s.id,
        s.category,
        s.brand,
        s.launch_date,
        s.tags,
        x.unit_price,
        x.order_item_quantity
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.unit_price, c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    CASE WHEN unit_price >= 380 THEN 'LARGE' ELSE 'SMALL' END AS unit_price_tier,
    COUNT(*) AS event_count,
    SIZE(COLLECT_SET(category)) AS num_categories,
    SUM(CASE WHEN category = 'Electronics' THEN unit_price ELSE 0 END) AS electronics_unit_price,
    SUM(CASE WHEN category = 'Fashion' THEN order_item_quantity ELSE 0 END) AS fashion_order_item_quantity,
    AVG(order_item_quantity) FILTER (WHERE brand = 'Initech') AS initech_avg_order_item_quantity,
    COUNT(*) AS num_rows
FROM with_latest
GROUP BY CASE WHEN unit_price >= 380 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY fashion_order_item_quantity DESC;
