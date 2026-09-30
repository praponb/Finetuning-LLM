WITH supplier_totals AS (
    SELECT supplier_id, SUM(cost) AS total_cost
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.supplier_name,
    s.country,
    st.total_cost
FROM suppliers s
JOIN supplier_totals st ON s.id = st.supplier_id
WHERE st.total_cost >= 1300;
