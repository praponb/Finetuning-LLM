WITH with_last AS (
    SELECT
        s.id,
        s.brand,
        s.price,
        latest.unit_price,
        latest.order_item_quantity
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.unit_price, c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    brand,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    COUNT(*) FILTER (WHERE brand = 'Umbrella') AS umbrella_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_last
GROUP BY 1
HAVING distinct_ids < 1000;
