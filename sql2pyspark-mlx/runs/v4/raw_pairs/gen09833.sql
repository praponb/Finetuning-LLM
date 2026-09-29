WITH supplier_totals AS (
    SELECT supplier_id, AVG(stock_qty) avg_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.rating,
    s.city,
    st.avg_stock_qty
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id
ORDER BY rating
