WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SIZE(tags) AS tags_count,
        SIZE(tags) AS tags_count_2
    FROM products
    WHERE brand IS NOT DISTINCT FROM 'Initech'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(price) OVER (PARTITION BY supplier_id ORDER BY tags_count, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_price,
        AVG(price) OVER (PARTITION BY supplier_id ORDER BY tags_count, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_price,
        MAX(price) OVER (PARTITION BY supplier_id ORDER BY tags_count, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_price
    FROM filtered
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.quantity != 7
        )
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        s.*,
        b.supplier_name,
        b.country,
        b.city
    FROM eligible s
    LEFT JOIN suppliers b
        ON s.supplier_id = b.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        launch_date,
        tags,
        tags_count,
        last3_max_price,
        supplier_name,
        country,
        city
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost NULLS LAST, id) AS rn
        FROM with_dims
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        category,
        price,
        tags_count,
        last3_max_price,
        COUNT(last3_max_price) OVER w_grp AS running_count_last3_max_price,
        MAX(last3_max_price) OVER w_grp AS running_max_last3_max_price,
        MIN(last3_max_price) OVER w_grp AS running_min_last3_max_price,
        AVG(last3_max_price) OVER w_grp AS running_avg_last3_max_price,
        AVG(last3_max_price) OVER (PARTITION BY city ORDER BY tags_count, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS last3_max_price_avg_5_prior
    FROM deduped
    WHERE ARRAY_CONTAINS(tags, 'eco')
    WINDOW w_grp AS (PARTITION BY city ORDER BY last3_max_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
stats AS (
    -- Multi-level aggregation
    SELECT
        category,
        COUNT_IF(category = 'Fashion') AS fashion_events,
        COUNT(CASE WHEN category = 'Home' THEN 1 END) AS home_count,
        COUNT(*) AS event_count,
        COUNT(CASE WHEN category = 'Sports' THEN 1 END) AS sports_count,
        PERCENTILE_APPROX(last3_max_price, ARRAY(0.25, 0.5, 0.75)) AS last3_max_price_quartiles,
        GROUPING(category) AS grouping_level
    FROM scored
    GROUP BY ROLLUP (category)
)
SELECT
    category,
    fashion_events,
    home_count,
    event_count,
    grouping_level
FROM stats
ORDER BY category DESC NULLS LAST;
