WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(supplier_name) clean_supplier_name,
        CAST(rating AS INT) rating_int
    FROM suppliers
    WHERE city IS DISTINCT FROM 'New York'
      AND city NOT IN ('London', 'Tokyo', 'Bangkok')
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.product_name NOT LIKE 'Ba%'
        )
),
eligible_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.stock_qty BETWEEN 200 AND 344
        )
),
filtered_suppliers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        city,
        rating,
        rating_int,
        LENGTH(city) city_length,
        COALESCE(city, 'UNKNOWN') resolved_city
    FROM eligible_suppliers
    WHERE city IS NOT NULL
),
eligible_suppliers_2 AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.launch_date <= '2024-03-01'
        )
)
SELECT
    id,
    city,
    rating,
    rating_int,
    city_length,
    resolved_city
FROM eligible_suppliers_2
ORDER BY city DESC NULLS LAST;
