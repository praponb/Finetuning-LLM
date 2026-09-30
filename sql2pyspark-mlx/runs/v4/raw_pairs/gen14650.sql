WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) AS rank_in_group
        FROM products
    ) ranked
    WHERE rank_in_group = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.unit_price BETWEEN 130 AND 300
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        rank_in_group,
        rank_in_group + cost AS rank_in_group_plus_cost
    FROM not_flagged
    WHERE launch_date > DATE '2024-03-12'
      AND supplier_id IS NOT NULL
      AND category IN ('Home', 'Sports')
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN price >= 1260 THEN 'LARGE' ELSE 'SMALL' END AS price_tier,
        category,
        COUNT(*) AS event_count,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS event_count_2,
        APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
        SUM(price) FILTER (WHERE brand = 'Acme') AS acme_total_price,
        SIZE(COLLECT_SET(category)) AS num_categories
    FROM cleaned
    GROUP BY 1, 2
)
SELECT
    price_tier,
    category,
    event_count,
    cost_quartiles,
    acme_total_price
FROM summary;
