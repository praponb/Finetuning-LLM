WITH scored AS (
    SELECT
        *,
        STDDEV_SAMP(quantity) OVER (PARTITION BY order_id) AS order_id_stddev_samp_quantity,
        PERCENT_RANK() OVER w_part AS unit_price_percent_rank,
        CORR(quantity, unit_price) OVER (PARTITION BY product_id) AS corr_quantity_unit_price
    FROM order_items
    WHERE unit_price BETWEEN 180 AND 220
    WINDOW w_part AS (PARTITION BY order_id ORDER BY unit_price DESC)
)
SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    order_id_stddev_samp_quantity,
    unit_price_percent_rank,
    corr_quantity_unit_price
FROM scored
ORDER BY product_id NULLS FIRST
