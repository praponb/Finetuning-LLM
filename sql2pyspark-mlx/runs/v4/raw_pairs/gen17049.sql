WITH with_windows AS (
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
        LEAD(price, 1) OVER (PARTITION BY brand ORDER BY price, id) AS next_price,
        NTILE(3) OVER (PARTITION BY supplier_id, brand ORDER BY stock_qty, id) AS stock_qty_bucket
    FROM products
),
enriched AS (
    SELECT
        e.*,
        d.city,
        d.supplier_name
    FROM with_windows e
    LEFT JOIN suppliers d
        ON e.supplier_id = d.id
),
base AS (
    SELECT
        *,
        DATE_SUB(launch_date, 9) AS launch_date_minus_9d,
        CASE WHEN next_price >= 1000 THEN 'High' WHEN next_price >= 380 THEN 'Medium' ELSE 'Low' END AS next_price_band
    FROM enriched
    WHERE launch_date >= DATE '2024-01-08'
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
    tags,
    CASE WHEN brand RLIKE '^(Acme|Globex)$' THEN 'CORE' WHEN brand RLIKE '^(Initech|Umbrella)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS brand_zone
FROM base
ORDER BY price DESC;
