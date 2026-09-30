SELECT
    id,
    plan,
    end_date,
    LAG(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id) AS prev_monthly_fee,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date, id) AS row_seq
FROM subscriptions
WHERE monthly_fee IS NOT NULL;
