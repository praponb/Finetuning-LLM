WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(rating) AS rating_whole,
        CONCAT(country, ', ', city) AS country_city,
        COALESCE(city, 'N/A') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating_whole, id) = 1
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.cost
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
filtered_suppliers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        city_filled,
        cost,
        UPPER(supplier_name) AS supplier_name_upper,
        COALESCE(city, 'NONE') AS resolved_city
    FROM with_latest
    WHERE city IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    country,
    metric,
    val
FROM (
    SELECT
        id,
        country,
        cost,
        rating
    FROM filtered_suppliers
) src
UNPIVOT (
    val FOR metric IN (cost AS COST_VAL, rating)
)
