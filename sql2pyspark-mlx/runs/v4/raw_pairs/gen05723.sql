WITH enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        o.status,
        o.order_date
    FROM order_items s
    LEFT JOIN orders o
        ON s.order_id = o.id
)
-- Multi-level aggregation
SELECT
    product_id,
    STDDEV_SAMP(unit_price) AS unit_price_stddev,
    SIZE(COLLECT_SET(status)) AS num_status,
    AVG(unit_price) AS avg_unit_price,
    COUNT(DISTINCT id) AS distinct_ids,
    BIT_OR(quantity) AS quantity_bit_or
FROM enriched
GROUP BY product_id;
