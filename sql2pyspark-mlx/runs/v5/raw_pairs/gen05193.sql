WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY last_restock ASC) AS dedup_rank
        FROM inventory
    ) ranked
    WHERE dedup_rank = 1
)
SELECT
    s.id,
    s.product_id,
    s.quantity,
    s.last_restock,
    (SELECT AVG(s2.revenue) FROM store_sales s2 WHERE s2.product_id = s.product_id) AS avg_store_sale_revenue
FROM ranked s
