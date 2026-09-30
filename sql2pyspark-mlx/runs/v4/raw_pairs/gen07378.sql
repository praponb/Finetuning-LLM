SELECT
    id,
    status,
    amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY status), 3) amount_share
FROM payments;
