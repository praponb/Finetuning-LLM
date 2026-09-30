WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        LAST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity
    FROM inventory
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.*,
        p.city,
        p.sqft,
        p.region
    FROM enriched src
    LEFT JOIN stores p
        ON src.store_id = p.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY cumulative_quantity DESC, id) AS dedup_rank
        FROM joined
    ) x
    WHERE dedup_rank = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY cumulative_quantity DESC NULLS LAST, id) = 1
),
enriched_inventory AS (
    -- Attach stores attributes
    SELECT a.*, d.region AS store_region
    FROM latest a
    LEFT JOIN stores d
        ON a.store_id = d.id
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN sqft >= 17089 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_tier,
        DATE_TRUNC('WEEK', last_restock) AS week_start,
        ROUND(SUM(cumulative_quantity) / NULLIF(COUNT(CASE WHEN city = 'Berlin' THEN 1 END), 0), 2) AS cumulative_quantity_per_berlin,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        ROUND(AVG(cumulative_quantity), 2) AS avg_cumulative_quantity,
        APPROX_PERCENTILE(last_known_quantity, ARRAY(0.25, 0.5, 0.75)) AS last_known_quantity_quartiles,
        ROUND(SUM(quantity) / NULLIF(COUNT(CASE WHEN store_region = 'Central' THEN 1 END), 0), 2) AS quantity_per_central
    FROM enriched_inventory
    GROUP BY GROUPING SETS ((CASE WHEN sqft >= 17089 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', last_restock)), (CASE WHEN sqft >= 17089 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
)
SELECT
    week_start,
    cumulative_quantity_per_berlin,
    city_list,
    avg_cumulative_quantity,
    last_known_quantity_quartiles
FROM daily
ORDER BY week_start DESC;
