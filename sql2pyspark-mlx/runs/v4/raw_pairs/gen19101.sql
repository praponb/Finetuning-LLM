WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        LOWER(supplier_name) AS supplier_name_lower,
        COALESCE(city, 'OTHER') AS city_filled
    FROM suppliers
    WHERE city = 'Chiang Mai'
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.stock_qty <= 112
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
              AND ARRAY_CONTAINS(x.tags, 'premium')
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        supplier_name_lower,
        city_filled,
        ROW_NUMBER() OVER (ORDER BY rating, id) AS seq_no,
        LAST_VALUE(rating, TRUE) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating,
        SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating
    FROM not_flagged
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.launch_date
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    country,
    rating,
    supplier_name_lower,
    city_filled,
    last_known_rating,
    launch_date,
    CASE WHEN country RLIKE '^(TH|UK)$' THEN 'CORE' ELSE 'GROUP_B' END AS country_zone
FROM latest_match
ORDER BY last_known_rating DESC;
