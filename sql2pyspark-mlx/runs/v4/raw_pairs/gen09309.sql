WITH supplier_totals AS (
    SELECT supplier_id, MAX(cost) - MIN(cost) AS cost_range
    FROM products
    GROUP BY supplier_id
)
SELECT s.supplier_name, st.cost_range
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id
WHERE st.cost_range > 42
ORDER BY s.supplier_name DESC;
