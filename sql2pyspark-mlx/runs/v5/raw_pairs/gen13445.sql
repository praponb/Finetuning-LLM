SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY order_date DESC) AS pos
    FROM orders
    WHERE channel NOT IN ('web', 'partner', 'store')
) AS ranked
WHERE pos <= 1
ORDER BY customer_id DESC;
