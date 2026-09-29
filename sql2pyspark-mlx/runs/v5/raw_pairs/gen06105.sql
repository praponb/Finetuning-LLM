WITH supplier_totals AS (
    SELECT supplier_id, COUNT(*) AS product_count
    FROM products
    GROUP BY supplier_id
)
SELECT s.country, st.product_count
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id
WHERE st.product_count >= 8;
