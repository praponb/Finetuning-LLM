WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units DESC, id) = 1
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.sale_date,
    s.revenue,
    (SELECT MIN(i2.quantity) FROM inventory i2 WHERE i2.store_id = s.store_id) AS min_inventory_quantity
FROM ranked s
ORDER BY revenue NULLS FIRST, store_id DESC
