WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        city,
        LOWER(city) AS city_lower,
        COALESCE(city, 'N/A') AS resolved_city
    FROM suppliers
    WHERE supplier_name LIKE 'Sia%'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category IN ('Sports', 'Books', 'Fashion')
        )
),
cleaned_suppliers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('/', city, supplier_name) AS city_supplier_name_key
    FROM not_flagged
    WHERE supplier_name IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(city, ', ', supplier_name) AS city_supplier_name,
        INITCAP(TRIM(city)) AS city_title
    FROM cleaned_suppliers
    WHERE city IS NOT NULL
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.stock_qty
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    supplier_name,
    city_lower,
    resolved_city,
    city_supplier_name,
    city_title,
    stock_qty,
    CASE WHEN supplier_name RLIKE '^(Global Supply|Pacific Wholesale)$' THEN 'GROUP_B' WHEN supplier_name RLIKE '^(Siam Parts Co|Tokyo Traders)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS supplier_name_zone
FROM with_latest
ORDER BY supplier_name_zone NULLS LAST;
