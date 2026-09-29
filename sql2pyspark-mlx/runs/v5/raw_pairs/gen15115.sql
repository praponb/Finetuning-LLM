SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    LAG(monthly_fee) OVER w_grp AS prev_monthly_fee,
    LEAD(monthly_fee) OVER w_grp AS next_monthly_fee,
    ROW_NUMBER() OVER w_grp AS row_seq,
    SUM(monthly_fee) OVER (PARTITION BY customer_id, plan) AS customer_id_sum_monthly_fee
FROM subscriptions
WINDOW w_grp AS (PARTITION BY customer_id ORDER BY start_date, id);
