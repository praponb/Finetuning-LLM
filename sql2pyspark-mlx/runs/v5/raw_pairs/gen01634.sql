WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        ARRAY_CONTAINS(tags, 'eco') is_eco,
        COALESCE(brand, 'UNKNOWN') resolved_brand
    FROM products
    WHERE supplier_id IS NOT NULL
      AND brand IS DISTINCT FROM 'Acme'
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.brand,
        s.price,
        s.cost,
        s.stock_qty,
        s.launch_date,
        s.tags,
        s.is_eco,
        s.resolved_brand,
        x.unit_price
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.product_id IN (18, 25, 39)
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC, id) seq_no,
        LEAD(price, 1, 0) OVER (PARTITION BY brand ORDER BY unit_price, id) next_price,
        FIRST_VALUE(cost, TRUE) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_cost,
        FIRST_VALUE(unit_price, TRUE) OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_unit_price
    FROM eligible
)
SELECT
    id,
    price,
    cost,
    stock_qty,
    launch_date,
    is_eco,
    unit_price,
    seq_no,
    first_known_cost
FROM with_windows;
