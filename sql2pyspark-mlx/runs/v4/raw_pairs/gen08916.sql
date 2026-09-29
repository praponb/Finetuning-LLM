WITH supplier_totals AS (
    SELECT supplier_id, SUM(cost) AS total_cost, SUM(CASE WHEN category = 'Home' THEN 1 ELSE 0 END) AS home_count
    FROM products
    GROUP BY supplier_id
)
SELECT s.rating, st.total_cost, st.home_count
FROM suppliers s
JOIN supplier_totals st ON s.id = st.supplier_id
WHERE st.home_count >= 36
ORDER BY st.total_cost, 1 DESC;
