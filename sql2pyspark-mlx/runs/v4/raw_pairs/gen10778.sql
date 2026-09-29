WITH store_totals AS (
    SELECT store_id, COUNT(revenue) revenue_count, MIN(units) min_units
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.region,
    s.store_name,
    st.revenue_count,
    st.min_units
FROM stores s
INNER JOIN store_totals st ON s.id = st.store_id
ORDER BY region DESC;
