WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        SORT_ARRAY(tags) AS sorted_tags,
        COALESCE(brand, 'UNKNOWN') AS resolved_brand
    FROM products
    WHERE launch_date > TO_DATE('2023-05-29')
      AND brand = 'Initech'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock < s.launch_date
        )
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_TRUNC('year', launch_date) AS launch_date_period,
        COALESCE(brand, 'UNKNOWN') AS resolved_brand_2
    FROM eligible
    WHERE cost BETWEEN 450 AND 490
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.order_item_unit_price,
        top1.quantity
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price, c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    category,
    brand,
    cost,
    stock_qty,
    launch_date,
    supplier_id,
    launch_date_period,
    order_item_unit_price
FROM with_last
ORDER BY launch_date_period NULLS LAST;
