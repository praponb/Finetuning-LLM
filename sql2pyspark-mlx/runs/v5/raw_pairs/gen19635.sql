WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CONCAT(supplier_name, ' ', city) AS supplier_name_city
    FROM suppliers
    WHERE city = 'New York'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.category = 'Home'
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_rating,
        MAX(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_rating,
        AVG(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_rating,
        SUM(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_rating,
        STDDEV_SAMP(rating) OVER (PARTITION BY city) AS city_stddev_samp_rating
    FROM kept
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        city,
        supplier_name_city,
        city_stddev_samp_rating
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY running_avg_rating, id) AS rank_in_group
        FROM metrics
    ) t
    WHERE rank_in_group = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.cost BETWEEN 430 AND 510
        )
)
-- Grouped metrics
SELECT
    city,
    COUNT_IF(city = 'London') AS london_events,
    COUNT_IF(city = 'Singapore') AS singapore_events,
    APPROX_PERCENTILE(city_stddev_samp_rating, ARRAY(0.25, 0.5, 0.75)) AS city_stddev_samp_rating_quartiles,
    MIN(city_stddev_samp_rating) AS min_city_stddev_samp_rating,
    APPROX_PERCENTILE(city_stddev_samp_rating, 0.95) AS city_stddev_samp_rating_p95
FROM eligible
GROUP BY city
ORDER BY min_city_stddev_samp_rating DESC NULLS FIRST
