SELECT *
FROM (
    SELECT method, MAX(amount) AS max_amount, AVG(amount) AS avg_amount
    FROM payments
    WHERE amount BETWEEN 500 AND 800
    GROUP BY method
) AS t
WHERE max_amount > 1530
ORDER BY avg_amount DESC;
