SELECT
    id,
    method,
    amount,
    paid_at
FROM (
    SELECT
        id,
        method,
        amount,
        paid_at,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS rnk
    FROM payments
) AS ranked
WHERE rnk <= 2;
