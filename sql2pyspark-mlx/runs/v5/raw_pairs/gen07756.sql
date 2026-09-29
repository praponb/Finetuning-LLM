WITH recent_products AS (
    SELECT *
    FROM products
    WHERE launch_date > '2023-10-01' AND product_name LIKE '%Bottle%'
),
Agg AS (
    SELECT supplier_id, category, COUNT(cost) AS cost_count
    FROM recent_products
    GROUP BY supplier_id, category
)
SELECT *
FROM Agg
ORDER BY category, supplier_id DESC;
