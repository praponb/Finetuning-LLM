WITH metrics AS (
    SELECT
        id,
        status,
        LAG(monthly_fee) OVER (PARTITION BY status, customer_id ORDER BY end_date, id) AS prev_monthly_fee,
        LEAD(monthly_fee) OVER (PARTITION BY status, customer_id ORDER BY end_date, id) AS next_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY end_date, id) AS row_seq,
        NTILE(10) OVER (PARTITION BY status ORDER BY monthly_fee, id) AS monthly_fee_bucket
    FROM subscriptions
)
SELECT
    id,
    prev_monthly_fee,
    monthly_fee_bucket
FROM metrics;
