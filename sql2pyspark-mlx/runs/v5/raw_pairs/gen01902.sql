WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        country,
        rating,
        CEIL(rating) rating_whole,
        COALESCE(city, 'OTHER') city_clean
    FROM suppliers
    WHERE supplier_name LIKE 'Glo%'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        country,
        city_clean,
        ROW_NUMBER() OVER (ORDER BY rating, id) event_seq,
        LEAD(rating, 1) OVER (PARTITION BY country ORDER BY rating_whole, id) next_rating,
        AVG(rating_whole) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) rating_whole_avg_6
    FROM filtered
    WHERE country = 'JP'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(next_rating) OVER (PARTITION BY country ORDER BY rating_whole_avg_6, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_next_rating,
        COUNT(next_rating) OVER (PARTITION BY country ORDER BY rating_whole_avg_6, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_next_rating,
        MIN(next_rating) OVER (PARTITION BY country ORDER BY rating_whole_avg_6, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_next_rating
    FROM scored
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(rating_whole_avg_6) IGNORE NULLS OVER w_ordered first_known_rating_whole_avg_6,
        NTILE(3) OVER (ORDER BY running_avg_next_rating, id) running_avg_next_rating_bucket,
        MIN(next_rating) OVER w_ordered_2 country_min_next_rating,
        NTILE(10) OVER w_part running_count_next_rating_bucket
    FROM enriched
    WINDOW w_ordered AS (ORDER BY running_min_next_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered_2 AS (PARTITION BY country), w_part AS (PARTITION BY country ORDER BY running_count_next_rating DESC, id)
)
-- Grouped metrics
SELECT
    CASE WHEN event_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
    country,
    CASE WHEN running_avg_next_rating >= 4.4 THEN 'HIGH' ELSE 'LOW' END AS running_avg_next_rating_tier,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    SIZE(COLLECT_SET(country)) AS num_countries,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
    COUNT(*) AS event_count
FROM with_windows
GROUP BY CASE WHEN event_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, country, CASE WHEN running_avg_next_rating >= 4.4 THEN 'HIGH' ELSE 'LOW' END
