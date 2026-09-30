WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(city)) AS city_title
    FROM suppliers
    WHERE city IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER w_ordered AS rating_percent_rank,
        FIRST_VALUE(rating, TRUE) OVER w_running AS first_known_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        COUNT(rating) OVER (PARTITION BY country) AS country_count_rating
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY city ORDER BY rating DESC), w_running AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(first_known_rating, 0) AS first_known_rating_filled,
        COALESCE(city, 'UNKNOWN') AS resolved_city
    FROM enriched
    WHERE city = 'Singapore'
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    city_title,
    rating_percent_rank,
    first_known_rating,
    country_count_rating,
    CASE WHEN supplier_name RLIKE '^(Siam Parts Co|EuroGoods GmbH)$' THEN 'CORE' ELSE 'GROUP_B' END AS supplier_name_zone
FROM valid
ORDER BY supplier_name_zone DESC NULLS FIRST, id DESC
LIMIT 5
