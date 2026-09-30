WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        city,
        REGEXP_REPLACE(supplier_name, ' ', '_') AS supplier_name_slug
    FROM suppliers
    WHERE city IS NOT NULL
      AND city IN ('New York', 'Chiang Mai', 'Singapore')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END AS is_tokyo,
        COALESCE(city, 'N/A') AS city_clean
    FROM base
    WHERE city IS NOT NULL
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.price >= 1180
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.brand = 'Stark'
        )
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.product_stock_qty
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
base_suppliers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        supplier_name_slug,
        product_stock_qty,
        COALESCE(supplier_name, 'Unknown') AS supplier_name_filled,
        LENGTH(supplier_name) AS supplier_name_length,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM latest_match
    WHERE city IS NOT NULL
)
-- Grouped metrics
SELECT
    city,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows,
    SUM(CASE WHEN city = 'London' THEN product_stock_qty ELSE 0 END) AS london_product_stock_qty,
    ROUND(SUM(supplier_name_length) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS supplier_name_length_per_chiang_mai,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT_IF(city = 'Tokyo') AS tokyo_events
FROM base_suppliers
GROUP BY city;
