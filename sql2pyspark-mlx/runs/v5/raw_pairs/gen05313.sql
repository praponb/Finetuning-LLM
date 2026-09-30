WITH valid_products AS (
    SELECT *, FLOOR(price) AS price_whole
    FROM products
    WHERE (stock_qty BETWEEN 382 AND 396 AND launch_date BETWEEN '2023-04-01' AND '2023-09-22') OR category = 'Grocery'
),
summary AS (
    SELECT supplier_id, MAX(cost) AS max_cost
    FROM valid_products
    GROUP BY supplier_id
)
SELECT supplier_id, max_cost
FROM summary
ORDER BY max_cost, supplier_id DESC
