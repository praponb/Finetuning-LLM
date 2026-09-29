WITH supplier_totals AS (
    SELECT supplier_id, MIN(stock_qty) AS min_stock_qty, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'new')
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    s.supplier_name,
    st.min_stock_qty,
    st.category_values
FROM suppliers s
LEFT OUTER JOIN supplier_totals st ON s.id = st.supplier_id
WHERE st.min_stock_qty > 280
ORDER BY st.min_stock_qty DESC, s.supplier_name ASC
