WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        INITCAP(TRIM(supplier_name)) AS supplier_name_title,
        LOWER(city) AS city_lower
    FROM suppliers
    WHERE city IS NOT NULL
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.cost
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last_suppliers AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.product_stock_qty
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(country, 1, 2) AS country_prefix,
        COALESCE(city, 'NONE') AS resolved_city
    FROM with_last_suppliers
    WHERE city IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY product_stock_qty NULLS LAST, id) AS rank_in_group
    FROM cleaned
    QUALIFY rank_in_group = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT country, id
    FROM deduped
    EXCEPT ALL
    SELECT country, id
    FROM suppliers
    WHERE country = 'SG'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.country = k.country AND s.id = k.id
)
SELECT
    supplier_name,
    country,
    city,
    supplier_name_title,
    city_lower,
    cost,
    product_stock_qty,
    rank_in_group
FROM reconciled
ORDER BY country DESC, product_stock_qty
