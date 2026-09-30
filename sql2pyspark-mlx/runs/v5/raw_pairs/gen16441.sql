WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS(',', city, supplier_name) AS city_supplier_name_key,
        UPPER(supplier_name) AS supplier_name_upper
    FROM suppliers
    WHERE country IN ('UK', 'US', 'JP')
),
valid_suppliers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(rating, 0) AS rating_filled,
        ROUND(rating * 0.85, 2) AS rating_adjusted
    FROM valid
    WHERE country != 'TH'
),
valid_suppliers_2 AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(rating_adjusted, rating_filled) AS greatest_rating_adjusted_rating_filled
    FROM valid_suppliers
    WHERE city IS NOT NULL
      AND city IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid_suppliers_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.cost BETWEEN 200 AND 650
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(rating) OVER w_ordered AS last4_max_rating,
        COUNT(rating) OVER w_ordered AS last4_count_rating,
        AVG(rating) OVER w_ordered AS last4_avg_rating,
        MIN(rating) OVER w_ordered AS last4_min_rating,
        NTILE(10) OVER (PARTITION BY country ORDER BY rating_filled DESC, id) AS rating_filled_bucket
    FROM not_flagged
    WHERE supplier_name NOT LIKE 'Gl%'
    WINDOW w_ordered AS (PARTITION BY city, country ORDER BY rating_filled, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN last4_count_rating >= 17 THEN 'LARGE' ELSE 'SMALL' END AS last4_count_rating_tier,
    country,
    CASE WHEN rating_filled_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_filled_bucket_tier,
    PERCENTILE_APPROX(last4_count_rating, 0.9) AS last4_count_rating_p90,
    COUNT(DISTINCT city) AS distinct_cities,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT_IF(city = 'London') AS london_events,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM enriched
GROUP BY CASE WHEN last4_count_rating >= 17 THEN 'LARGE' ELSE 'SMALL' END, country, CASE WHEN rating_filled_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING london_events > 1
ORDER BY london_events NULLS FIRST
