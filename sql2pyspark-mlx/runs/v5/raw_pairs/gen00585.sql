SELECT *
FROM (
    SELECT status, AVG(amount) AS avg_amount
    FROM payments
    GROUP BY status
) AS agg
WHERE avg_amount < 1100
ORDER BY status
