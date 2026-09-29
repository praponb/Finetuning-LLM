WITH top_products AS (
    SELECT *, RANK() OVER (PARTITION BY supplier_id ORDER BY cost DESC) AS pos
    FROM products
)
SELECT
    id,
    supplier_id,
    cost,
    launch_date
FROM top_products
WHERE pos = 1
ORDER BY id DESC
