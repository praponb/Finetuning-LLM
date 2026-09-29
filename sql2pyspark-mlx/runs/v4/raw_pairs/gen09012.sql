SELECT
    id,
    method,
    paid_at,
    amount
FROM (
    SELECT
        id,
        method,
        paid_at,
        amount,
        DENSE_RANK() OVER (PARTITION BY method ORDER BY paid_at DESC) AS rnk
    FROM payments
) AS r
WHERE rnk <= 1
ORDER BY method, 3;
