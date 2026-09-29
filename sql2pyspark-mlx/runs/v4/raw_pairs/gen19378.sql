WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(rating, 1, 0) OVER (ORDER BY rating, id) AS prev_rating,
        SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS event_seq,
        NTILE(3) OVER (PARTITION BY country ORDER BY rating DESC, id) AS rating_bucket
    FROM suppliers
),
with_windows_suppliers AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(rating_bucket, TRUE) OVER (PARTITION BY country ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating_bucket,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating_bucket, id) AS row_seq,
        FIRST_VALUE(event_seq, TRUE) OVER (PARTITION BY city ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_seq,
        COUNT(rating) OVER (PARTITION BY city) AS city_count_rating
    FROM with_windows
),
with_windows_suppliers_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        prev_rating,
        running_rating,
        event_seq,
        rating_bucket,
        last_known_rating_bucket,
        row_seq,
        first_known_event_seq,
        city_count_rating,
        FIRST_VALUE(city, TRUE) OVER (PARTITION BY city ORDER BY last_known_rating_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        MIN(running_rating) OVER w_hist AS country_min_running_rating
    FROM with_windows_suppliers
    WINDOW w_hist AS (PARTITION BY country)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        prev_rating,
        running_rating,
        event_seq,
        rating_bucket,
        last_known_rating_bucket,
        row_seq,
        first_known_event_seq,
        city_count_rating,
        first_known_city,
        country_min_running_rating,
        AVG(rating_bucket) OVER (ORDER BY rating_bucket, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rating_bucket_avg_3,
        AVG(country_min_running_rating) OVER (ORDER BY country_min_running_rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS country_min_running_rating_avg_2,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating_bucket, id) AS event_seq_2,
        SUM(row_seq) OVER (ORDER BY last_known_rating_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_row_seq
    FROM with_windows_suppliers_2
)
-- Multi-level aggregation
SELECT
    country,
    CASE WHEN last_known_rating_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END AS last_known_rating_bucket_tier,
    APPROX_PERCENTILE(prev_rating, ARRAY(0.25, 0.5, 0.75)) AS prev_rating_quartiles,
    PERCENTILE_APPROX(row_seq, 0.5) AS row_seq_p50,
    PERCENTILE_APPROX(running_rating, ARRAY(0.25, 0.5, 0.75)) AS running_rating_quartiles,
    STDDEV_SAMP(rating) AS rating_stddev,
    ROUND(SUM(event_seq_2) / NULLIF(COUNT(CASE WHEN country = 'DE' THEN 1 END), 0), 2) AS event_seq_2_per_de,
    SUM(CASE WHEN country = 'SG' THEN city_count_rating ELSE 0 END) AS sg_city_count_rating
FROM scored
GROUP BY 1, 2
HAVING sg_city_count_rating < 0
ORDER BY rating_stddev;
