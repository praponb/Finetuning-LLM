WITH filtered AS (
    SELECT
        id,
        product_id,
        unit_price,
        ROUND(quantity, 1) quantity_rounded
    FROM order_items
    WHERE unit_price IS NOT NULL
)
SELECT
    id,
    product_id,
    unit_price,
    NTILE(4) OVER w_part unit_price_bucket,
    SUM(quantity_rounded) OVER (PARTITION BY product_id ORDER BY quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_quantity_rounded,
    AVG(quantity_rounded) OVER (ORDER BY quantity_rounded, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) quantity_rounded_avg_3_prior
FROM filtered
WHERE unit_price < 280
WINDOW w_part AS (PARTITION BY product_id ORDER BY unit_price, id)
