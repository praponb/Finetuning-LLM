WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        SUM(units) OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units,
        LAG(revenue, 1) OVER (ORDER BY sale_date, id) AS prev_revenue
    FROM store_sales
    WHERE sale_date <= '2024-08-01'
)
SELECT
    s.id,
    s.product_id,
    s.units,
    s.running_units,
    s.prev_revenue,
    (SELECT MIN(i2.quantity) FROM inventory i2 WHERE i2.product_id = s.product_id) AS min_inventory_quantity
FROM enriched s
ORDER BY min_inventory_quantity NULLS LAST;
