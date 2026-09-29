SELECT
    id,
    order_id,
    amount
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount, id) AS rn
    FROM payments
) x
WHERE rn = 1;
