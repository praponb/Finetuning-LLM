WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        TRIM(country) AS clean_country
    FROM suppliers
    WHERE city IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT country
    FROM filtered
    EXCEPT
    SELECT country
    FROM suppliers
    WHERE rating <= 3.3
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN matched_keys k ON s.country = k.country
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.brand = 'Acme'
        )
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN country = 'TH' THEN 1 ELSE 0 END AS is_th,
        UPPER(country) AS country_upper
    FROM eligible
    WHERE supplier_name IS NOT NULL
)
SELECT
    id,
    supplier_name,
    is_th,
    CASE WHEN supplier_name RLIKE '^(Global Supply|Pacific Wholesale)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS supplier_name_zone
FROM cleaned
ORDER BY id DESC, is_th DESC
