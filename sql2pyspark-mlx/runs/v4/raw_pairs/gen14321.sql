WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        city,
        rating,
        ROUND(rating, 1) rating_rounded
    FROM suppliers
    WHERE city IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(city, TRUE) OVER w_part last_known_city,
        CUME_DIST() OVER w_running rating_cume_dist,
        MIN(rating_rounded) OVER (PARTITION BY city) city_min_rating_rounded
    FROM filtered
    WINDOW w_part AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY city ORDER BY rating NULLS LAST)
),
enriched_suppliers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY city ORDER BY rating_rounded DESC, id) rating_rounded_bucket,
        LAST_VALUE(city_min_rating_rounded) IGNORE NULLS OVER (PARTITION BY city ORDER BY rating_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_city_min_rating_rounded,
        LAG(city_min_rating_rounded, 2) OVER (ORDER BY city_min_rating_rounded, id) prev_city_min_rating_rounded,
        AVG(city_min_rating_rounded) OVER (ORDER BY city_min_rating_rounded, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) city_min_rating_rounded_avg_3
    FROM enriched
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched_suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY city_min_rating_rounded_avg_3, id) = 1
)
SELECT
    id,
    city,
    rating,
    rating_rounded,
    last_known_city,
    rating_cume_dist,
    city_min_rating_rounded,
    rating_rounded_bucket,
    prev_city_min_rating_rounded
FROM deduped
ORDER BY rating_rounded
