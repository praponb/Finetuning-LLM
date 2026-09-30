WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.category = 'Sports'
        )
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM kept
    QUALIFY RANK() OVER (PARTITION BY country ORDER BY rating DESC NULLS FIRST) = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC NULLS FIRST, id) AS row_num
        FROM latest
    ) t
    WHERE row_num = 1
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.product_launch_date
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_last
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY row_num, id) = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(country, '/', city) AS country_city,
        TRIM(city) AS clean_city,
        COALESCE(city, 'DIRECT') AS city_clean
    FROM deduped
    WHERE city IS NOT NULL
      AND rating >= 2.9
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        row_num - rating AS row_num_minus_rating,
        CASE WHEN city IN ('Bangkok', 'Berlin') THEN 'Group A' ELSE 'Other' END AS city_group,
        COALESCE(city, 'OTHER') AS city_clean_2
    FROM filtered
    WHERE rating IS NOT NULL
)
-- Multi-level aggregation
SELECT
    country,
    MIN(rating) AS min_rating,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    STDDEV_SAMP(rating) AS rating_stddev,
    COUNT(CASE WHEN city = 'Berlin' THEN 1 END) AS berlin_count,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
    APPROX_PERCENTILE(row_num_minus_rating, 0.95) AS row_num_minus_rating_p95
FROM base
GROUP BY 1
