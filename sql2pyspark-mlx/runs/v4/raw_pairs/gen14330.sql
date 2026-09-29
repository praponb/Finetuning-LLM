WITH store_totals AS (
    SELECT store_id, COUNT(quantity) AS quantity_count
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.id,
    s.opened_date,
    s.region,
    st.quantity_count
FROM stores s
LEFT JOIN store_totals st ON s.id = st.store_id
ORDER BY s.opened_date;
