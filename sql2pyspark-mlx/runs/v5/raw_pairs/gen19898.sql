WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN rating >= 4.1 THEN 'High' WHEN rating >= 2.3 THEN 'Medium' ELSE 'Low' END rating_band
    FROM suppliers
    WHERE city IS NOT NULL
      AND rating <= 3.0
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        rating,
        latest_rank,
        AVG(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) rating_avg_3_prior,
        CUME_DIST() OVER (PARTITION BY city, country ORDER BY rating DESC) rating_cume_dist
    FROM latest
    WHERE latest_rank <= 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY rating_avg_3_prior) rating_avg_3_prior_dense_rank,
        LAST_VALUE(rating_avg_3_prior) IGNORE NULLS OVER (ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_rating_avg_3_prior,
        CUME_DIST() OVER (PARTITION BY city ORDER BY rating_cume_dist DESC NULLS FIRST) rating_cume_dist_cume_dist
    FROM metrics
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN rating_cume_dist_cume_dist >= 0.76 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_cume_dist_cume_dist_tier,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(*) AS row_count,
    ROUND(SUM(rating_cume_dist_cume_dist) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS rating_cume_dist_cume_dist_per_new_york
FROM enriched
GROUP BY CASE WHEN rating_cume_dist_cume_dist >= 0.76 THEN 'PREMIUM' ELSE 'STANDARD' END;
