WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN city IN ('Tokyo', 'Singapore') THEN 'Group A' ELSE 'Other' END AS city_group,
        COALESCE(city, 'N/A') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating, id) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.category = 'Home'
        )
),
filtered_suppliers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        city_group,
        city_filled,
        rank_in_group,
        TRIM(supplier_name) AS clean_supplier_name,
        REPLACE(supplier_name, ' ', '') AS supplier_name_slug,
        COALESCE(city, 'DIRECT') AS city_filled_2
    FROM eligible
    WHERE rank_in_group <= 1
),
eligible_suppliers AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered_suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND UPPER(TRIM(ch.product_name)) = 'OFFICE CHAIR'
        )
)
SELECT
    id,
    supplier_name,
    city,
    rating,
    city_group,
    city_filled,
    rank_in_group,
    clean_supplier_name
FROM eligible_suppliers
ORDER BY city DESC NULLS LAST, rank_in_group;
