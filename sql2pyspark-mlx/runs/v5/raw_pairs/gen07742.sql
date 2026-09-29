SELECT *
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY order_date DESC) AS pos
    FROM orders
) AS t
WHERE pos <= 3;
