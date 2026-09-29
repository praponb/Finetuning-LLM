WITH store_totals AS (
    SELECT store_id, MIN(sale_date) AS first_sale_date
    FROM store_sales
    GROUP BY store_id
)
SELECT s.store_name, s.city, st.first_sale_date
FROM stores s
LEFT OUTER JOIN store_totals st ON s.id = st.store_id;
