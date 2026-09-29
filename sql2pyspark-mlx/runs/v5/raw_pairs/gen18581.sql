SELECT *
FROM (
    SELECT
        customer_id,
        status,
        monthly_fee
    FROM subscriptions
) src
PIVOT (
    MAX(monthly_fee) AS amount,
    COUNT(1) AS n
    FOR status IN ('EXPIRED' AS expired, 'CANCELLED' AS cancelled)
);
