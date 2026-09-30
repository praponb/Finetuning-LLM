SELECT status, method, num_payments
FROM (
    SELECT status, method, COUNT(*) AS num_payments
    FROM payments
    GROUP BY status, method
) AS s
WHERE num_payments > 9
ORDER BY num_payments
