WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN quantity >= 194 THEN 'High' WHEN quantity >= 119 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM inventory
    WHERE last_restock BETWEEN '2024-09-18' AND '2025-01-01'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity DESC, id) AS rank_in_group
    FROM cleaned
    QUALIFY rank_in_group = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC, id) AS latest_rank
        FROM deduped
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.category,
        p.launch_date,
        p.brand
    FROM latest src
    INNER JOIN products p
        ON src.product_id = p.id
),
joined AS (
    -- Attach products attributes
    SELECT a.*, d.category AS product_category
    FROM enriched a
    INNER JOIN products d
        ON a.product_id = d.id
    WHERE d.price > 890
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
        DATE_TRUNC('MONTH', launch_date) AS activity_month,
        SIZE(COLLECT_SET(category)) AS num_categories,
        ROUND(AVG(quantity), 2) AS avg_quantity,
        STDDEV_SAMP(quantity) AS quantity_stddev,
        STDDEV_SAMP(quantity) AS quantity_stddev_2
    FROM joined
    GROUP BY quarter_start, activity_month
    HAVING STDDEV_SAMP(quantity) < 1000000
)
SELECT
    quarter_start,
    activity_month,
    num_categories,
    quantity_stddev_2
FROM stats;
