WITH base AS (
    SELECT *, UPPER(method) AS method_upper
    FROM payments
    WHERE method = 'promptpay' AND amount < 480
),
summary AS (
    SELECT HOUR(paid_at) AS paid_hour, MIN(paid_at) AS min_paid_at, COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count
    FROM base
    GROUP BY HOUR(paid_at)
)
SELECT paid_hour, min_paid_at, refunded_count
FROM summary;
