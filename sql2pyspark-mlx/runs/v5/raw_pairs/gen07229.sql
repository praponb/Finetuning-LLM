WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(country) country_length,
        TRIM(country) clean_country,
        COALESCE(city, 'NONE') city_filled
    FROM suppliers
    WHERE supplier_name LIKE '%Glob%'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        supplier_name,
        city,
        rating,
        country_length
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY country_length DESC NULLS FIRST, id) AS latest_rank
        FROM filtered
    ) x
    WHERE latest_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        country_length,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) row_seq,
        STDDEV_SAMP(rating) OVER w_part city_stddev_samp_rating,
        AVG(country_length) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) country_length_avg_2,
        CUME_DIST() OVER (PARTITION BY city ORDER BY rating NULLS LAST) rating_cume_dist,
        STDDEV_SAMP(country_length) OVER w_part city_stddev_samp_country_length
    FROM latest
    WINDOW w_part AS (PARTITION BY city)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.cost <= 590
        )
)
-- Running and moving aggregates
SELECT
    id,
    row_seq,
    country_length_avg_2,
    COUNT(rating_cume_dist) OVER w_grp running_count_rating_cume_dist,
    SUM(rating_cume_dist) OVER w_grp running_sum_rating_cume_dist,
    AVG(rating_cume_dist) OVER w_grp running_avg_rating_cume_dist,
    MIN(rating_cume_dist) OVER w_grp running_min_rating_cume_dist,
    NTILE(10) OVER w_running country_length_avg_2_bucket
FROM not_flagged
WINDOW w_grp AS (PARTITION BY city ORDER BY country_length_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY city ORDER BY country_length_avg_2 DESC, id);
