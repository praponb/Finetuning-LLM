WITH filtered AS (
    SELECT
        *,
        quantity + unit_price AS line_total,
        quantity * unit_price AS line_total_2
    FROM order_items
    WHERE product_id IS NOT NULL
)
SELECT
    *,
    AVG(unit_price) OVER (PARTITION BY order_id ORDER BY line_total_2 ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS unit_price_avg_5,
    CUME_DIST() OVER (ORDER BY line_total DESC) AS line_total_cume_dist,
    AVG(unit_price) OVER (PARTITION BY order_id, product_id ORDER BY line_total_2 ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS unit_price_avg_5_prior
FROM filtered;
