SELECT
    *,
    NTILE(4) OVER (ORDER BY monthly_fee DESC, id ASC) AS monthly_fee_bucket,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee ASC, id ASC) AS seq_no,
    NTILE(3) OVER (ORDER BY monthly_fee ASC, id ASC) AS monthly_fee_bucket_2,
    LAG(monthly_fee, 1, 0) OVER (PARTITION BY status ORDER BY start_date ASC, id ASC) AS prev_monthly_fee
FROM subscriptions;
