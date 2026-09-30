WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        last_restock,
        DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label,
        COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE last_restock >= DATE '2023-06-24'
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p2.city,
        p2.region
    FROM cleaned s
    INNER JOIN stores p2
        ON s.store_id = p2.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        last_restock,
        quantity_filled,
        city,
        region
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, region ORDER BY quantity_filled DESC NULLS LAST, id) = 1
),
ranked_inventory AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY quantity_filled, id) AS rn
    FROM ranked
    QUALIFY rn = 1
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN quantity_filled >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_filled_tier,
        DATE_TRUNC('MONTH', last_restock) AS activity_month,
        PERCENTILE_APPROX(quantity_filled, ARRAY(0.25, 0.5, 0.75)) AS quantity_filled_quartiles,
        SIZE(COLLECT_SET(region)) AS num_regions,
        ROUND(AVG(quantity_filled), 2) AS avg_quantity_filled
    FROM ranked_inventory
    GROUP BY CASE WHEN quantity_filled >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', last_restock)
)
SELECT
    quantity_filled_tier,
    activity_month,
    num_regions,
    avg_quantity_filled
FROM totals
ORDER BY activity_month NULLS LAST
