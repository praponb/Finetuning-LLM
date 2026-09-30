WITH supplier_totals AS (
    SELECT supplier_id, MAX(cost) - MIN(cost) AS cost_range
    FROM products
    GROUP BY supplier_id
)
SELECT s.city, s.rating, st.cost_range
FROM suppliers s
LEFT OUTER JOIN supplier_totals st ON s.id = st.supplier_id
WHERE st.cost_range >= 58
ORDER BY st.cost_range DESC;
