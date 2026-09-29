WITH store_totals AS (
    SELECT store_id, ROUND(STDDEV(units), 2) AS stddev_units
    FROM store_sales
    WHERE units < 39
    GROUP BY store_id
)
SELECT s.opened_date, st.stddev_units
FROM stores s
INNER JOIN store_totals st ON s.id = st.store_id
ORDER BY s.opened_date ASC
