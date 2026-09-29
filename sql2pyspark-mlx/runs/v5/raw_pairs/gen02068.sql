SELECT
    id,
    customer_id,
    plan,
    end_date,
    monthly_fee,
    LEAD(monthly_fee) OVER w_part AS next_monthly_fee,
    LAG(monthly_fee) OVER w_part AS prev_monthly_fee,
    ROW_NUMBER() OVER w_part AS event_seq,
    AVG(monthly_fee) OVER (PARTITION BY status, customer_id) AS status_avg_monthly_fee
FROM subscriptions
WHERE monthly_fee <= 90
WINDOW w_part AS (PARTITION BY status ORDER BY monthly_fee, id);
