WITH remaining_keys AS (
    SELECT id, supplier_id
    FROM products
    EXCEPT DISTINCT
    SELECT id, supplier_id
    FROM products
    WHERE launch_date < '2023-08-01'
),
kept_rows AS (
    SELECT s.*
    FROM products s
    JOIN remaining_keys k ON s.id = k.id AND s.supplier_id = k.supplier_id
),
eligible AS (
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.quantity <= 186
        )
),
remaining_keys_2 AS (
    SELECT id
    FROM eligible
    INTERSECT ALL
    SELECT id
    FROM products
    WHERE cost BETWEEN 650 AND 670
),
reconciled AS (
    SELECT s.*
    FROM eligible s
    JOIN remaining_keys_2 k ON s.id = k.id
),
filtered AS (
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        CAST(price AS INT) AS price_int,
        COALESCE(brand, 'NONE') AS brand_clean
    FROM reconciled
    WHERE ARRAY_CONTAINS(tags, 'premium')
),
base AS (
    SELECT
        id,
        brand,
        price,
        launch_date,
        price_int,
        brand_clean,
        ROUND(cost * 100, 2) AS cost_adjusted
    FROM filtered
    WHERE launch_date >= DATE '2023-06-06'
      AND price_int IS NOT NULL
),
base_products AS (
    SELECT
        id,
        brand,
        launch_date,
        price_int,
        ROUND(price * 1.1, 2) AS adjusted_price
    FROM base
    WHERE launch_date >= '2024-10-01'
),
totals AS (
    SELECT
        brand,
        COUNT(*) FILTER (WHERE brand = 'Globex') AS globex_rows,
        PERCENTILE_APPROX(adjusted_price, ARRAY(0.25, 0.5, 0.75)) AS adjusted_price_quartiles,
        COUNT(CASE WHEN brand = 'Globex' THEN 1 END) AS globex_count,
        COUNT(DISTINCT id) AS distinct_ids
    FROM base_products
    GROUP BY brand
    HAVING globex_count >= 1
)
SELECT
    brand,
    globex_rows,
    adjusted_price_quartiles,
    distinct_ids,
    CASE WHEN brand RLIKE '^(Initech|Stark)$' THEN 'GROUP_A' WHEN brand RLIKE '^(Acme|Globex)$' THEN 'GROUP_B' ELSE 'CORE' END AS brand_zone
FROM totals;
