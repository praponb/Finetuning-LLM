WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM order_items
    WHERE order_id IN (14, 26)
),
joined AS (
    -- Attach orders attributes
    SELECT e.*, b.channel
    FROM filtered e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY quantity_filled DESC) AS quantity_filled_cume_dist,
        NTILE(10) OVER (PARTITION BY order_id, channel ORDER BY quantity, id) AS quantity_bucket,
        SUM(quantity_filled) OVER (PARTITION BY channel, order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_filled,
        FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id, order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY quantity DESC, id) AS event_seq
    FROM joined
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        channel,
        quantity_filled_cume_dist,
        quantity_bucket,
        cumulative_quantity_filled,
        event_seq,
        CEIL(quantity_filled_cume_dist) AS quantity_filled_cume_dist_whole
    FROM enriched
    WHERE product_id IS NOT NULL
      AND channel IS DISTINCT FROM 'web'
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN quantity_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS quantity_bucket_tier,
        SUM(quantity_bucket) AS total_quantity_bucket,
        BIT_OR(quantity) AS quantity_bit_or,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(quantity_filled_cume_dist_whole, ARRAY(0.25, 0.5, 0.75)) AS quantity_filled_cume_dist_whole_quartiles
    FROM base
    GROUP BY CASE WHEN quantity_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    quantity_bucket_tier,
    quantity_bit_or,
    row_count,
    quantity_filled_cume_dist_whole_quartiles
FROM summary
ORDER BY quantity_bucket_tier, row_count DESC
