WITH with_windows AS (
    SELECT
        id,
        order_id,
        quantity,
        SUM(quantity) OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_quantity,
        COUNT(quantity) OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_quantity,
        MAX(quantity) OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity,
        MIN(quantity) OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_quantity,
        FIRST_VALUE(unit_price, TRUE) OVER (ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price
    FROM order_items
)
SELECT
    CASE WHEN quantity >= 9 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    AVG(quantity) AS avg_quantity,
    ROUND(AVG(running_sum_quantity), 2) AS avg_running_sum_quantity
FROM with_windows
GROUP BY CASE WHEN quantity >= 9 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY quantity_tier DESC
