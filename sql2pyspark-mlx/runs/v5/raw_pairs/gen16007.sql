WITH store_totals AS (
    SELECT store_id, MAX(quantity) AS max_quantity, COUNT(quantity) AS quantity_count
    FROM inventory
    WHERE product_id IN (1, 3, 4, 7)
    GROUP BY store_id
)
SELECT
    s.id,
    s.region,
    s.city,
    st.max_quantity,
    st.quantity_count
FROM stores s
INNER JOIN store_totals st ON s.id = st.store_id
ORDER BY s.id ASC
