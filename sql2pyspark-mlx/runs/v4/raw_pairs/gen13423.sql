WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date DESC, id) AS event_seq,
        LAST_VALUE(monthly_fee) IGNORE NULLS OVER w_running AS last_known_monthly_fee,
        DENSE_RANK() OVER w_recent AS monthly_fee_dense_rank,
        LAG(monthly_fee, 1, 0) OVER w_running_2 AS prev_monthly_fee
    FROM subscriptions
    WINDOW w_running AS (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY customer_id ORDER BY monthly_fee DESC NULLS FIRST), w_running_2 AS (PARTITION BY plan, customer_id ORDER BY start_date, id)
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM metrics a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.end_date >= sub.start_date
       AND a.end_date < sub.end_date + INTERVAL '30' DAY
),
kept_keys AS (
    -- Set difference on keys
    SELECT plan, id
    FROM attributed
    EXCEPT DISTINCT
    SELECT plan, id
    FROM subscriptions
    WHERE plan <> 'basic'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM attributed s
    JOIN kept_keys k ON s.plan = k.plan AND s.id = k.id
)
SELECT
    s.id,
    s.customer_id,
    s.start_date,
    s.end_date,
    s.status,
    s.event_seq,
    s.last_known_monthly_fee,
    s.prev_monthly_fee,
    s.subscription_id,
    CASE WHEN status RLIKE '^(PAUSED|ACTIVE)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS status_zone,
    (SELECT MAX(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS max_account_balance
FROM kept_rows s
ORDER BY max_account_balance NULLS LAST, end_date
