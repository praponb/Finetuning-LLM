WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        TRIM(supplier_name) AS clean_supplier_name
    FROM suppliers
    WHERE city IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(supplier_name) AS clean_supplier_name_2,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM filtered
    WHERE city IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.category <> 'Fashion'
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.brand NOT IN ('Initech', 'Stark', 'Acme')
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand = 'Umbrella'
        )
)
SELECT
    id,
    supplier_name,
    country,
    city,
    clean_supplier_name,
    city_filled
FROM eligible
ORDER BY id DESC
LIMIT 20;
