WITH valid AS (
    SELECT *, CAST(monthly_fee AS INT) AS monthly_fee_int
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND status <> 'ACTIVE'
)
SELECT
    id,
    plan,
    monthly_fee,
    status,
    monthly_fee_int,
    LAG(monthly_fee) OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee, id) AS prev_monthly_fee,
    LEAD(monthly_fee) OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee, id) AS next_monthly_fee,
    LEAD(monthly_fee_int) OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee, id) AS next_monthly_fee_int,
    ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee, id) AS event_seq,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share
FROM valid;
