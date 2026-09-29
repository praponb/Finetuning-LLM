WITH with_windows AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC) AS quantity_rank,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY unit_price DESC, id ASC) AS unit_price_bucket
    FROM order_items
)
SELECT
    product_id,
    order_id,
    BIT_OR(quantity) AS quantity_bit_or,
    PERCENTILE_APPROX(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    BIT_OR(quantity) AS quantity_bit_or_2,
    CORR(quantity, unit_price) AS corr_quantity_unit_price,
    APPROX_PERCENTILE(quantity_rank, 0.5) AS quantity_rank_p50,
    CORR(quantity, unit_price) AS corr_quantity_unit_price_2
FROM with_windows
GROUP BY GROUPING SETS ((product_id, order_id), (product_id))
HAVING corr_quantity_unit_price_2 > 0
ORDER BY quantity_bit_or_2 ASC;
