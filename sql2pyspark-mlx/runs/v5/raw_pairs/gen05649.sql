WITH enriched AS (
    SELECT
        e.id,
        e.launch_date,
        e.supplier_id,
        d.rating,
        d.city
    FROM products e
    LEFT JOIN suppliers d
        ON e.supplier_id <=> d.id
),
enriched_products AS (
    SELECT
        id,
        rating,
        city,
        MIN(rating) OVER (PARTITION BY city ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_rating,
        MAX(rating) OVER (PARTITION BY city ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_rating,
        SUM(rating) OVER (PARTITION BY city ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_rating
    FROM enriched
),
filtered AS (
    SELECT
        id,
        rating,
        city,
        running_max_rating,
        CASE WHEN running_min_rating >= 4.2 THEN 'High' WHEN running_min_rating >= 1.5 THEN 'Medium' ELSE 'Low' END AS running_min_rating_tier,
        FLOOR(running_sum_rating) AS running_sum_rating_whole,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM enriched_products
    WHERE city IN ('Singapore', 'Chiang Mai')
),
remaining_keys AS (
    SELECT id
    FROM filtered
    EXCEPT
    SELECT id
    FROM products
    WHERE brand != 'Stark'
),
reconciled AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.id = k.id
),
with_windows AS (
    SELECT
        *,
        ROUND(running_max_rating / SUM(running_max_rating) OVER (PARTITION BY city), 4) AS running_max_rating_share,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY running_sum_rating_whole DESC NULLS LAST) AS running_sum_rating_whole_percent_rank
    FROM reconciled
)
SELECT
    city,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    PERCENTILE_APPROX(running_max_rating, 0.95) AS running_max_rating_p95
FROM with_windows
GROUP BY city
ORDER BY city
