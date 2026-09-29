WITH store_totals AS (
    SELECT store_id, COUNT(DISTINCT quantity) AS unique_products
    FROM inventory
    WHERE quantity <= 60
    GROUP BY store_id
)
SELECT s.opened_date, s.city, st.unique_products
FROM stores s
INNER JOIN store_totals st ON s.id = st.store_id
ORDER BY 2 DESC
