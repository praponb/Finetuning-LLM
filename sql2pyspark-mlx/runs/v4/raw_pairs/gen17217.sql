SELECT *
FROM (
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount, id) AS pos
    FROM payments
) AS ranked
WHERE pos = 1
