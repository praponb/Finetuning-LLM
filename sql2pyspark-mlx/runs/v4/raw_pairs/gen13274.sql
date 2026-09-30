WITH base AS (
    SELECT *
    FROM payments
    WHERE TO_DATE(paid_at) >= '2024-08-01'
),
agg AS (
    SELECT
        method,
        CASE WHEN method IN ('bank_transfer', 'paypal') THEN 'tier_1' ELSE 'other' END AS method_group,
        COUNT(amount) AS amount_count,
        AVG(amount) AS avg_amount
    FROM base
    GROUP BY method, CASE WHEN method IN ('bank_transfer', 'paypal') THEN 'tier_1' ELSE 'other' END
)
SELECT
    method,
    method_group,
    amount_count,
    avg_amount
FROM agg
WHERE avg_amount < 1360;
