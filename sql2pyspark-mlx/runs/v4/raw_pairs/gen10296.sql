SELECT *
FROM (
    SELECT HOUR(paid_at) AS paid_hour, AVG(amount) AS avg_amount
    FROM payments
    GROUP BY HOUR(paid_at)
) AS x
WHERE avg_amount > 590
ORDER BY paid_hour DESC;
