SELECT
    id,
    method,
    amount,
    status
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount, id) AS rnk
    FROM payments
) AS ranked
WHERE rnk = 1
ORDER BY id DESC;
