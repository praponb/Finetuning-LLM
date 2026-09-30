WITH filtered AS (
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status,
        CEIL(monthly_fee) monthly_fee_whole
    FROM subscriptions
    WHERE end_date >= DATE '2023-10-12'
      AND customer_id IS NOT NULL
)
SELECT *
FROM (
    SELECT
        customer_id,
        status,
        monthly_fee_whole
    FROM filtered
) src
PIVOT (
    SUM(monthly_fee_whole) AS amount
    FOR status IN ('ACTIVE', 'EXPIRED', 'PAUSED')
);
