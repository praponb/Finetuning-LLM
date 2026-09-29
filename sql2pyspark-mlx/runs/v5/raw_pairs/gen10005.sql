WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units, id) AS rn
        FROM store_sales
    ) x
    WHERE rn = 1
)
SELECT
    s.id,
    s.store_id,
    s.units,
    s.revenue,
    s.rn,
    (SELECT MIN(x.quantity) FROM inventory x WHERE x.store_id = s.store_id) AS min_inventory_quantity
FROM latest s
