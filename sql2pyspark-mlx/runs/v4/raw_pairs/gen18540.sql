WITH cleaned AS (
    SELECT
        *,
        ROUND(price * 0.9, 2) AS price_scaled,
        CASE WHEN category = 'Fashion' THEN 1 ELSE 0 END AS is_fashion,
        COALESCE(brand, 'N/A') AS brand_filled
    FROM products
    WHERE price > 450
),
latest_match AS (
    SELECT
        s.*,
        latest.unit_price,
        latest.order_item_quantity
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.unit_price, c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    SELECT *
    FROM latest_match
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category, brand ORDER BY cost DESC NULLS LAST, id ASC) = 1
)
SELECT
    id,
    product_name,
    category,
    price,
    cost,
    stock_qty,
    launch_date,
    tags,
    price_scaled,
    CASE WHEN product_name RLIKE '^(Backpack XL|Coffee Maker)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS product_name_zone
FROM deduped
ORDER BY stock_qty ASC NULLS FIRST, product_name_zone ASC
