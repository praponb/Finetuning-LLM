WITH with_dims AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.stock_qty,
        src.tags,
        src.supplier_id,
        b.rating,
        b.city,
        b.supplier_name
    FROM products src
    INNER JOIN suppliers b
        ON src.supplier_id <=> b.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        stock_qty,
        tags,
        supplier_id,
        rating,
        city,
        COALESCE(city, 'N/A') AS city_filled,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM with_dims
    WHERE supplier_id IN (16, 30)
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.tags,
        s.rating,
        s.city_filled,
        s.resolved_city,
        latest.order_item_unit_price,
        latest.quantity
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price, c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.store_id IN (4, 5)
        )
)
SELECT
    id,
    tags,
    rating,
    city_filled,
    resolved_city,
    order_item_unit_price
FROM not_flagged
ORDER BY rating DESC
