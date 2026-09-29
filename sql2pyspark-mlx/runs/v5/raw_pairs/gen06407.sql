WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(rating, 0) AS rating_rounded,
        ROUND(rating, 1) AS rating_rounded_2,
        COALESCE(city, 'N/A') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        rating_rounded,
        rating_rounded_2,
        city_filled,
        FLOOR(rating_rounded_2) AS rating_rounded_2_whole
    FROM cleaned
    WHERE city IS NOT NULL
      AND rating < 3.5
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        rating_rounded_2,
        city_filled,
        rating_rounded_2_whole,
        CASE WHEN rating_rounded >= 63 THEN 'High' WHEN rating_rounded >= 32 THEN 'Medium' ELSE 'Low' END AS rating_rounded_band,
        COALESCE(city, 'DIRECT') AS city_clean
    FROM base
    WHERE city IS NOT NULL
      AND rating_rounded_2 <= 38
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        country,
        city_filled,
        city_clean
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating_rounded_2_whole DESC, id) AS rn
        FROM valid
    ) x
    WHERE rn = 1
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.stock_qty
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.product_name IS NULL
        )
)
SELECT
    id,
    country,
    city_filled,
    stock_qty,
    CASE WHEN country RLIKE '^(SG|JP)$' THEN 'PRIORITY' ELSE 'CORE' END AS country_zone
FROM kept
ORDER BY id NULLS LAST;
