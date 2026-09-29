SELECT
    id,
    plan,
    start_date,
    end_date,
    status,
    AVG(monthly_fee) OVER (ORDER BY monthly_fee, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_5_prior,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date, id) AS row_seq,
    LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee
FROM subscriptions;
