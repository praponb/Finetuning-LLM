SELECT
    id,
    order_id,
    amount,
    paid_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS rnk
    FROM payments
) AS ranked
WHERE rnk = 1
ORDER BY paid_at;
