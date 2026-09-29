WITH OrdersByStore AS (
    SELECT store_id, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM orders
    GROUP BY store_id
)
SELECT s.id, s.sqft, O.stddev_amount
FROM stores s
LEFT JOIN OrdersByStore O ON s.id = O.store_id
WHERE O.stddev_amount > 51
ORDER BY O.stddev_amount DESC, 1 DESC;
