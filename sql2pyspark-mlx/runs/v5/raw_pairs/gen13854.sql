WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, quantity - unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.discount,
        b.channel
    FROM cleaned src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        line_total,
        discount,
        channel,
        TRIM(channel) AS clean_channel
    FROM enriched
    WHERE channel IS NOT NULL
      AND discount >= 0.18
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        unit_price,
        line_total,
        discount,
        channel,
        clean_channel,
        CASE WHEN channel = 'store' THEN 1 ELSE 0 END AS is_store
    FROM filtered
    WHERE line_total IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN discount >= 0.32 THEN 'HIGH' ELSE 'LOW' END AS discount_tier,
    COUNT_IF(channel = 'web') AS web_events,
    SUM(CASE WHEN channel = 'store' THEN discount ELSE 0 END) AS store_discount,
    PERCENTILE_APPROX(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    COUNT_IF(channel = 'mobile') AS mobile_events,
    APPROX_PERCENTILE(line_total, ARRAY(0.25, 0.5, 0.75)) AS line_total_quartiles
FROM base
GROUP BY 1
ORDER BY mobile_events ASC NULLS FIRST;
