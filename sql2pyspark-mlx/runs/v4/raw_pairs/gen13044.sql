WITH supplier_totals AS (
    SELECT supplier_id, MAX(price) AS max_price, COUNT(DISTINCT stock_qty) AS unique_categorys
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.city,
    s.supplier_name,
    st.max_price,
    st.unique_categorys
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id
ORDER BY city DESC, unique_categorys;
