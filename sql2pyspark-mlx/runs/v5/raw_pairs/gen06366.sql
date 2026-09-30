SELECT
    id,
    paid_at,
    amount,
    DATE_TRUNC('MONTH', paid_at) AS paid_at_period
FROM payments
WHERE TO_DATE(paid_at) >= '2023-05-27' AND (amount < 1000 OR amount <= 750);
