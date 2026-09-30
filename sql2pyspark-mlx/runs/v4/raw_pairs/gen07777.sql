WITH filtered AS (
    SELECT *, ROUND(unit_price * 0.9, 2) AS unit_price_adjusted
    FROM order_items
    WHERE product_id = 39
)
SELECT
    *,
    CUME_DIST() OVER (PARTITION BY product_id ORDER BY unit_price DESC NULLS LAST) AS unit_price_cume_dist,
    SUM(unit_price) OVER (PARTITION BY product_id, order_id) AS product_id_sum_unit_price,
    RANK() OVER (PARTITION BY product_id, order_id ORDER BY quantity DESC NULLS FIRST) AS quantity_rank
FROM filtered
