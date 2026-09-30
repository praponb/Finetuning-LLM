SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    NTILE(3) OVER w_part AS monthly_fee_bucket,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share,
    ROW_NUMBER() OVER (ORDER BY start_date, id) AS row_seq
FROM subscriptions
WINDOW w_part AS (PARTITION BY plan ORDER BY monthly_fee DESC, id);
