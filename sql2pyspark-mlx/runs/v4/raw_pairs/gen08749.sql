WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) row_seq,
        FIRST_VALUE(unit_price, TRUE) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_unit_price,
        AVG(unit_price) OVER (ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) unit_price_avg_2_prior,
        AVG(unit_price) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) unit_price_avg_5_prior,
        NTILE(3) OVER (ORDER BY quantity, id) quantity_bucket
    FROM order_items
)
SELECT
    id,
    row_seq,
    first_known_unit_price,
    unit_price_avg_5_prior,
    quantity_bucket
FROM with_windows
ORDER BY quantity_bucket DESC NULLS LAST
