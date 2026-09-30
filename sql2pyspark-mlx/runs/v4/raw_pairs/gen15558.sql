SELECT
    id,
    order_id,
    amount,
    method
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) rnk
    FROM payments
    WHERE order_id IS NOT NULL
) r
WHERE rnk = 1
ORDER BY id DESC;
