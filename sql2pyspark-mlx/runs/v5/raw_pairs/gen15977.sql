WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        rating,
        REGEXP_REPLACE(supplier_name, ' ', '-') AS supplier_name_slug,
        REGEXP_REPLACE(supplier_name, ' ', '_') AS supplier_name_slug_2,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        rating,
        LAST_VALUE(rating, TRUE) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating,
        NTILE(5) OVER (PARTITION BY city ORDER BY rating DESC, id) AS rating_bucket
    FROM cleaned
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.launch_date >= '2025-04-14'
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(rating) OVER (PARTITION BY city ORDER BY rating, id) AS prev_rating,
        LEAD(rating_bucket) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating_bucket,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS row_seq,
        LEAD(last_known_rating, 1) OVER (ORDER BY rating, id) AS next_last_known_rating
    FROM eligible
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        city,
        CASE WHEN next_last_known_rating >= 4.5 THEN 'HIGH' ELSE 'LOW' END AS next_last_known_rating_tier,
        MAX(rating_bucket) AS max_rating_bucket,
        ROUND(AVG(row_seq), 2) AS avg_row_seq,
        COUNT(*) AS event_count,
        SIZE(COLLECT_SET(city)) AS num_cities,
        PERCENTILE_APPROX(row_seq, 0.5) AS row_seq_p50,
        AVG(rating_bucket) AS avg_rating_bucket
    FROM enriched
    GROUP BY city, CASE WHEN next_last_known_rating >= 4.5 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    city,
    next_last_known_rating_tier,
    max_rating_bucket,
    avg_row_seq,
    event_count,
    row_seq_p50,
    avg_rating_bucket
FROM grouped
ORDER BY avg_row_seq NULLS FIRST, row_seq_p50;
