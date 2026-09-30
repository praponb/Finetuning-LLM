SELECT *
FROM (
    SELECT method, MAX(paid_at) AS latest_paid_at, ROUND(AVG(amount), 2) AS avg_amount
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 10 AND 13
    GROUP BY method
) AS agg
WHERE avg_amount < 44
