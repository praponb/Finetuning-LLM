WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        REPLACE(product_name, ' ', '') AS product_name_slug,
        COALESCE(brand, 'OTHER') AS brand_filled
    FROM products
    WHERE launch_date >= TO_DATE('2024-01-29')
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.order_id IN (7, 17, 33, 36)
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        product_name_slug,
        brand_filled,
        LAST_VALUE(brand) IGNORE NULLS OVER (PARTITION BY category, supplier_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_brand,
        AVG(price) OVER (PARTITION BY category ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS price_avg_3_prior
    FROM not_flagged
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.units
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    tags
FROM latest_match
