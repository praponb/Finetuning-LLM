WITH store_totals AS (
    SELECT store_id, MIN(sale_date) AS first_sale_date
    FROM store_sales
    GROUP BY store_id
)
SELECT s.sqft, st.first_sale_date
FROM stores s
JOIN store_totals st ON s.id = st.store_id
ORDER BY sqft DESC, st.first_sale_date
