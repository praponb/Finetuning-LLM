SELECT method, avg_amount, first_paid_at
FROM (
    SELECT method, AVG(amount) avg_amount, MIN(paid_at) first_paid_at
    FROM payments
    GROUP BY method
) sub
WHERE avg_amount > 870;
