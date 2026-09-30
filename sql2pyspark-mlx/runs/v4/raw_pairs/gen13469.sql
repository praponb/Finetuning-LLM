WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        city,
        rating,
        CASE WHEN country = 'TH' THEN 1 ELSE 0 END is_th,
        COALESCE(city, 'UNKNOWN') resolved_city
    FROM suppliers
    WHERE rating BETWEEN 2.7 AND 3.8
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(is_th, TRUE) OVER (PARTITION BY city ORDER BY is_th, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_is_th,
        SUM(rating) OVER w_recent running_rating,
        LAG(rating, 1) OVER (PARTITION BY city ORDER BY is_th, id) prev_rating
    FROM base
    WINDOW w_recent AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER w_recent running_rating_bucket,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) rating_share
    FROM scored
    WINDOW w_recent AS (PARTITION BY city ORDER BY running_rating DESC, id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        city,
        CASE WHEN first_known_is_th >= 69 THEN 'HIGH' ELSE 'LOW' END AS first_known_is_th_tier,
        COUNT(*) AS event_count,
        COUNT(*) AS event_count_2,
        SUM(CASE WHEN city = 'Chiang Mai' THEN rating ELSE 0 END) AS chiang_mai_rating
    FROM with_windows
    GROUP BY city, CASE WHEN first_known_is_th >= 69 THEN 'HIGH' ELSE 'LOW' END
    HAVING COUNT(*) < 1
)
SELECT
    city,
    first_known_is_th_tier,
    event_count,
    CASE WHEN city RLIKE '^(Tokyo|Chiang Mai)$' THEN 'GROUP_A' ELSE 'CORE' END AS city_zone
FROM grouped
