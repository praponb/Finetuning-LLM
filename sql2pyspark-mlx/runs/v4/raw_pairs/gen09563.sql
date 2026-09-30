WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_SUB(last_restock, 7) AS last_restock_minus_7d,
        CEIL(quantity) AS quantity_whole
    FROM inventory
    WHERE store_id = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        p.brand,
        p.price,
        p.category
    FROM filtered s
    LEFT JOIN products p
        ON s.product_id = p.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_minus_7d NULLS FIRST, id) AS rank_in_group
        FROM joined
    ) ranked
    WHERE rank_in_group = 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN quantity >= 194 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
        DATE_TRUNC('YEAR', last_restock) AS year_start,
        DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
        COUNT(*) AS event_count,
        COUNT_IF(brand = 'Umbrella') AS umbrella_events,
        APPROX_PERCENTILE(quantity_whole, ARRAY(0.25, 0.5, 0.75)) AS quantity_whole_quartiles
    FROM deduped
    GROUP BY CASE WHEN quantity >= 194 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', last_restock), DATE_TRUNC('QUARTER', last_restock)
    HAVING event_count < 1
)
SELECT
    quantity_tier,
    year_start,
    quarter_start,
    event_count
FROM stats
