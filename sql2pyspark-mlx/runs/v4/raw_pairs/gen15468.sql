WITH supplier_totals AS (
    SELECT supplier_id, COUNT(DISTINCT price) AS unique_categorys, MAX(launch_date) AS last_launch_date
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.supplier_name,
    st.unique_categorys,
    st.last_launch_date
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id
