WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS latest_rank
    FROM order_items
    QUALIFY latest_rank = 1
)
SELECT
    order_id,
    APPROX_PERCENTILE(unit_price, 0.9) AS unit_price_p90,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    COUNT(*) AS num_rows,
    COUNT(*) AS cnt,
    COUNT(*) AS num_rows_2
FROM latest
GROUP BY order_id
