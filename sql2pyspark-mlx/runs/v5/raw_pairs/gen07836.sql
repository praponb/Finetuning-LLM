WITH valid AS (
    SELECT
        *,
        CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END AS is_berlin,
        COALESCE(city, 'N/A') AS city_clean
    FROM suppliers
    WHERE city IS NOT NULL
      AND city IS DISTINCT FROM 'Bangkok'
),
with_latest AS (
    SELECT
        s.id,
        s.supplier_name,
        x.launch_date,
        x.product_cost
    FROM valid s
    JOIN LATERAL (
            SELECT c.launch_date, c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    SELECT
        s.id,
        s.supplier_name,
        latest.product_brand
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest_suppliers AS (
    SELECT
        s.id,
        s.supplier_name,
        s.product_brand,
        top1.product_stock_qty
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
summary AS (
    SELECT
        CASE WHEN product_stock_qty >= 307 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_stock_qty_tier,
        product_brand,
        SUM(product_stock_qty) FILTER (WHERE product_brand = 'Initech') AS initech_total_product_stock_qty,
        COUNT_IF(product_brand = 'Stark') AS stark_events
    FROM with_latest_suppliers
    GROUP BY CASE WHEN product_stock_qty >= 307 THEN 'PREMIUM' ELSE 'STANDARD' END, product_brand
)
SELECT
    product_stock_qty_tier,
    product_brand,
    initech_total_product_stock_qty
FROM summary
ORDER BY product_stock_qty_tier;
