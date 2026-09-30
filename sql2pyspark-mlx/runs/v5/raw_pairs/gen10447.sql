SELECT method, stddev_amount, latest_paid_at
FROM (
    SELECT method, ROUND(STDDEV(amount), 2) AS stddev_amount, MAX(paid_at) AS latest_paid_at
    FROM payments
    GROUP BY method
) AS x
WHERE stddev_amount > 63
