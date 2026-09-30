WITH store_sales_by_product AS (
    SELECT product_id, AVG(units) AS avg_units, MIN(revenue) AS min_revenue
    FROM store_sales
    GROUP BY product_id
)
SELECT
    p.id,
    p.supplier_id,
    ssbp.avg_units,
    ssbp.min_revenue
FROM products p
LEFT OUTER JOIN store_sales_by_product ssbp ON p.id = ssbp.product_id
WHERE ssbp.min_revenue > 2600
ORDER BY avg_units ASC
