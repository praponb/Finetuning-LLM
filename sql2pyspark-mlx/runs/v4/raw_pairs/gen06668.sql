WITH supplier_totals AS (
    SELECT supplier_id, MIN(launch_date) first_launch_date
    FROM products
    WHERE cost IS NOT NULL
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.supplier_name,
    s.rating,
    st.first_launch_date
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id
