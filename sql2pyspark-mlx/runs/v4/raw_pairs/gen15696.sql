SELECT status, min_amount, cnt
FROM (
    SELECT status, MIN(amount) AS min_amount, COUNT(*) AS cnt
    FROM payments
    GROUP BY status
) AS x
WHERE min_amount > 900
