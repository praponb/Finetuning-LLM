SELECT id, order_id, amount
FROM (
    SELECT
        id,
        order_id,
        amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) pos
    FROM payments
) ranked
WHERE pos <= 3
