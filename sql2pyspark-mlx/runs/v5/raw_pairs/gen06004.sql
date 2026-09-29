WITH product_totals AS (
    SELECT product_id, AVG(revenue) AS avg_revenue, MIN(sale_date) AS first_sale_date
    FROM store_sales
    WHERE revenue <= 2400
    GROUP BY product_id
)
SELECT
    p.id,
    p.launch_date,
    p.stock_qty,
    pt.avg_revenue,
    pt.first_sale_date
FROM products p
INNER JOIN product_totals pt ON p.id = pt.product_id
ORDER BY pt.avg_revenue DESC
