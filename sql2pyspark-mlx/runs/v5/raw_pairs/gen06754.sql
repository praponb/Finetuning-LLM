WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) AS rnk
    FROM store_sales
)
SELECT
    id,
    product_id,
    revenue,
    sale_date
FROM numbered
WHERE rnk <= 2
ORDER BY revenue DESC
