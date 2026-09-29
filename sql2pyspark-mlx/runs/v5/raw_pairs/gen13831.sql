WITH store_summary AS (
    SELECT store_id, ROUND(STDDEV(units), 2) AS stddev_units, COUNT(DISTINCT units) AS unique_stores
    FROM store_sales
    WHERE units <= 15
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    ss.stddev_units,
    ss.unique_stores
FROM stores s
JOIN store_summary ss ON s.id = ss.store_id
ORDER BY id, ss.stddev_units DESC;
