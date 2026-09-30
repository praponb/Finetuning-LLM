WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(supplier_name) supplier_name_lower,
        REGEXP_REPLACE(supplier_name, ' ', '') supplier_name_slug,
        COALESCE(city, 'UNKNOWN') city_filled
    FROM suppliers
    WHERE city IN ('Bangkok', 'London')
),
base_suppliers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        rating,
        supplier_name_lower,
        supplier_name_slug,
        ROUND(rating * 1.07, 2) rating_adjusted
    FROM base
    WHERE city <> 'New York'
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.country,
        s.supplier_name_lower,
        s.rating_adjusted,
        top1.stock_qty,
        top1.product_launch_date
    FROM base_suppliers s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty, c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.country,
        s.rating_adjusted,
        s.stock_qty,
        s.product_launch_date,
        x.product_launch_date_2
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date_2
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    country,
    rating_adjusted,
    stock_qty,
    product_launch_date,
    product_launch_date_2
FROM with_latest
ORDER BY rating_adjusted DESC NULLS FIRST;
