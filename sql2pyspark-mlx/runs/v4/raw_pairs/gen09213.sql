WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(quantity / unit_price, 2) line_total,
        COALESCE(unit_price, 0) unit_price_filled
    FROM order_items
    WHERE unit_price <= 300
)
-- Multi-level aggregation
SELECT
    order_id,
    product_id,
    STDDEV_SAMP(unit_price) AS unit_price_stddev,
    COUNT(*) AS event_count,
    BIT_OR(quantity) AS quantity_bit_or
FROM cleaned
GROUP BY order_id, product_id
HAVING quantity_bit_or < 1;
