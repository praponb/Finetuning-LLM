SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC, id) AS rnk
    FROM orders
) AS ranked
WHERE rnk <= 3
ORDER BY amount DESC
