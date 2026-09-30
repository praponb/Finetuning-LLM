WITH latest AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY quantity NULLS FIRST) AS rn
        FROM order_items
    ) ranked
    WHERE rn = 1
)
SELECT
    product_id,
    STDDEV_SAMP(unit_price) AS unit_price_stddev,
    BIT_AND(quantity) AS quantity_bit_and,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles
FROM latest
GROUP BY product_id
HAVING STDDEV_SAMP(unit_price) < 1000000
ORDER BY unit_price_stddev DESC NULLS FIRST
