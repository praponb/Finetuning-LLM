WITH store_totals AS (
    SELECT store_id, SUM(amount) AS total_amount, MAX(order_date) AS last_order_date
    FROM orders
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    s.region,
    st.total_amount,
    st.last_order_date
FROM stores s
LEFT OUTER JOIN store_totals st ON s.id = st.store_id
WHERE st.total_amount >= 5600
ORDER BY s.region DESC, store_name
