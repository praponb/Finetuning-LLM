WITH store_sales_by_product AS (
    SELECT product_id, MAX(revenue) AS max_revenue
    FROM store_sales
    GROUP BY product_id
)
SELECT p.id, p.cost, ssbp.max_revenue
FROM products p
INNER JOIN store_sales_by_product ssbp ON p.id = ssbp.product_id
ORDER BY p.cost DESC;
