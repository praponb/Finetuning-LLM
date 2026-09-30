WITH kept_keys AS (
    SELECT store_id, id
    FROM store_sales
    EXCEPT DISTINCT
    SELECT store_id, id
    FROM store_sales
    WHERE revenue <= 2900
),
verified AS (
    SELECT s.*
    FROM store_sales s
    JOIN kept_keys k ON s.store_id = k.store_id AND s.id = k.id
)
SELECT
    s.product_id,
    s.sale_date,
    s.units,
    s.revenue,
    (SELECT MIN(t2.quantity) FROM inventory t2 WHERE t2.product_id = s.product_id) AS min_inventory_quantity
FROM verified s
ORDER BY units NULLS LAST, product_id DESC;
