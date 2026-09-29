WITH latest_match AS (
    SELECT
        s.id,
        s.category,
        s.brand,
        s.launch_date,
        s.tags,
        s.supplier_id,
        x.quantity
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    CASE WHEN quantity >= 252 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    AVG(quantity) FILTER (WHERE category = 'Grocery') AS grocery_avg_quantity,
    COUNT(*) AS cnt,
    ROUND(AVG(quantity), 2) AS avg_quantity
FROM latest_match
GROUP BY 1
