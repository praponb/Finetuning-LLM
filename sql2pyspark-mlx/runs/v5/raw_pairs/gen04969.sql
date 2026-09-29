WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        ROUND(rating * 1.15, 2) AS rating_scaled,
        CASE WHEN city IN ('Berlin', 'Bangkok') THEN 'Core' ELSE 'Other' END AS city_group
    FROM suppliers
    WHERE city = 'Tokyo'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        country,
        city,
        rating,
        rating_scaled,
        city_group,
        FIRST_VALUE(city) IGNORE NULLS OVER w_hist AS first_known_city,
        FIRST_VALUE(city, TRUE) OVER w_hist AS first_known_city_2
    FROM filtered
    WINDOW w_hist AS (PARTITION BY country ORDER BY rating_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(rating_scaled) OVER w_running AS running_min_rating_scaled,
        COUNT(rating_scaled) OVER w_running AS running_count_rating_scaled,
        MAX(rating_scaled) OVER w_running AS running_max_rating_scaled
    FROM enriched
    WINDOW w_running AS (PARTITION BY city ORDER BY rating_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_suppliers AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        rating_scaled,
        city_group,
        first_known_city,
        running_min_rating_scaled,
        LAG(rating_scaled) OVER (PARTITION BY city, country ORDER BY running_max_rating_scaled, id) AS prev_rating_scaled,
        LEAD(rating_scaled) OVER (PARTITION BY city, country ORDER BY running_max_rating_scaled, id) AS next_rating_scaled,
        LAG(running_min_rating_scaled) OVER (PARTITION BY city, country ORDER BY running_max_rating_scaled, id) AS prev_running_min_rating_scaled,
        LEAD(running_min_rating_scaled) OVER (PARTITION BY city, country ORDER BY running_max_rating_scaled, id) AS next_running_min_rating_scaled,
        ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY running_max_rating_scaled, id) AS seq_no
    FROM with_windows
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN next_rating_scaled >= 61 THEN 'HIGH' ELSE 'LOW' END AS next_rating_scaled_tier,
        seq_no,
        APPROX_PERCENTILE(prev_running_min_rating_scaled, 0.9) AS prev_running_min_rating_scaled_p90,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT_IF(city = 'Bangkok') AS bangkok_events,
        COUNT(*) AS cnt,
        SUM(CASE WHEN city = 'New York' THEN prev_running_min_rating_scaled ELSE 0 END) AS new_york_prev_running_min_rating_scaled
    FROM enriched_suppliers
    GROUP BY GROUPING SETS ((CASE WHEN next_rating_scaled >= 61 THEN 'HIGH' ELSE 'LOW' END, seq_no), (CASE WHEN next_rating_scaled >= 61 THEN 'HIGH' ELSE 'LOW' END))
)
SELECT
    next_rating_scaled_tier,
    seq_no,
    prev_running_min_rating_scaled_p90,
    bangkok_events,
    new_york_prev_running_min_rating_scaled
FROM grouped
ORDER BY bangkok_events DESC;
