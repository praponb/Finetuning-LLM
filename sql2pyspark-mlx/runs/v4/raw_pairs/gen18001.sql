WITH product_summary AS (
    SELECT product_id, MAX(revenue) AS max_revenue, MAX(sale_date) AS latest_sale_date
    FROM store_sales
    WHERE units < 30
    GROUP BY product_id
)
SELECT
    p.launch_date,
    p.stock_qty,
    ps.max_revenue,
    ps.latest_sale_date
FROM products p
LEFT JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.max_revenue > 3500
