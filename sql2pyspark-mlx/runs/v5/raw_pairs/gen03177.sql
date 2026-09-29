WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.product_name IS NOT NULL
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_rating,
        MAX(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_rating,
        SUM(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_rating
    FROM kept
    WHERE LENGTH(supplier_name) >= 10
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY country ORDER BY running_max_rating, id) AS running_max_rating_bucket,
        FIRST_VALUE(city) IGNORE NULLS OVER (ORDER BY running_sum_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        ROUND(running_count_rating / SUM(running_count_rating) OVER (PARTITION BY city), 4) AS running_count_rating_share,
        FIRST_VALUE(running_max_rating) IGNORE NULLS OVER (PARTITION BY country ORDER BY running_max_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_max_rating
    FROM metrics
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY running_count_rating_share NULLS FIRST) AS rn
    FROM enriched
    QUALIFY rn = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.brand = 'Globex'
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_count_rating >= 17 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_count_rating_tier,
    city,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
    COUNT(CASE WHEN city = 'Berlin' THEN 1 END) AS berlin_count,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN running_count_rating >= 17 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(CASE WHEN city = 'Berlin' THEN 1 END) DESC) AS berlin_count_percent_rank
FROM eligible
GROUP BY 1, 2
HAVING num_cities > 1
ORDER BY num_cities DESC NULLS LAST
