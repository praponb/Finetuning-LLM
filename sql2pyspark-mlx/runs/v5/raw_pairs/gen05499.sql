SELECT status, success_amount
FROM (
    SELECT status, SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) AS success_amount
    FROM payments
    GROUP BY status
) AS t
WHERE success_amount >= 70
ORDER BY success_amount;
