WITH filtered_products AS (
    SELECT *
    FROM products
    WHERE (supplier_id IS NOT NULL AND stock_qty > 227) OR cost BETWEEN 340 AND 600
),
summary AS (
    SELECT supplier_id, ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM filtered_products
    GROUP BY supplier_id
)
SELECT supplier_id, stddev_cost
FROM summary;
