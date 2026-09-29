WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        FLOOR(rating) AS rating_whole,
        LOWER(supplier_name) AS supplier_name_lower,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM suppliers
    WHERE city IS DISTINCT FROM 'London'
      AND city = 'Bangkok'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        country,
        supplier_name_lower,
        city_filled,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_rating,
        AVG(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_rating,
        COUNT(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_rating,
        MAX(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_rating,
        FIRST_VALUE(city, TRUE) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city
    FROM cleaned
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        country,
        last3_avg_rating,
        last3_max_rating,
        SUM(last3_avg_rating) OVER (PARTITION BY country) AS country_sum_last3_avg_rating,
        SUM(last3_count_rating) OVER w_hist AS running_last3_count_rating
    FROM with_windows
    WHERE country NOT IN ('SG', 'UK', 'TH')
    WINDOW w_hist AS (PARTITION BY country ORDER BY last3_max_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.last3_avg_rating,
        s.last3_max_rating,
        s.country_sum_last3_avg_rating,
        s.running_last3_count_rating,
        latest.cost
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        last3_max_rating,
        country_sum_last3_avg_rating,
        running_last3_count_rating,
        cost,
        CAST(last3_max_rating AS INT) AS last3_max_rating_int
    FROM with_latest
    WHERE country_sum_last3_avg_rating >= 8
)
-- Ranking and gap features
SELECT
    id,
    last3_max_rating,
    last3_max_rating_int,
    RANK() OVER (ORDER BY country_sum_last3_avg_rating NULLS LAST) AS country_sum_last3_avg_rating_rank,
    AVG(country_sum_last3_avg_rating) OVER (ORDER BY running_last3_count_rating, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS country_sum_last3_avg_rating_avg_3_prior,
    CUME_DIST() OVER (ORDER BY last3_max_rating DESC NULLS LAST) AS last3_max_rating_cume_dist,
    AVG(cost) OVER (ORDER BY running_last3_count_rating, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS cost_avg_5
FROM filtered;
