WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ABS(balance) AS abs_balance
    FROM accounts
    WHERE opened_date > DATE '2023-06-20'
      AND status IS NOT NULL
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM cleaned a
    INNER JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.opened_date >= sub.start_date
       AND a.opened_date < sub.end_date + INTERVAL 7 DAYS
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        abs_balance,
        subscription_id,
        plan,
        monthly_fee,
        COUNT(monthly_fee) OVER w_ordered AS running_count_monthly_fee,
        SUM(monthly_fee) OVER w_ordered AS running_sum_monthly_fee,
        MAX(monthly_fee) OVER w_ordered AS running_max_monthly_fee
    FROM attributed
    WINDOW w_ordered AS (PARTITION BY plan ORDER BY subscription_id, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time >= s.opened_date
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        balance,
        status,
        abs_balance,
        subscription_id,
        plan,
        running_sum_monthly_fee,
        NTILE(5) OVER (PARTITION BY plan ORDER BY running_count_monthly_fee, id, subscription_id) AS running_count_monthly_fee_bucket,
        COUNT(monthly_fee) OVER w_grp AS account_type_count_monthly_fee,
        COUNT(running_count_monthly_fee) OVER (PARTITION BY plan) AS plan_count_running_count_monthly_fee
    FROM not_flagged
    WHERE subscription_id >= 4
    WINDOW w_grp AS (PARTITION BY account_type)
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        account_type,
        MIN(abs_balance) AS min_abs_balance,
        APPROX_PERCENTILE(plan_count_running_count_monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS plan_count_running_count_monthly_fee_quartiles,
        COUNT_IF(plan = 'standard') AS standard_events
    FROM enriched
    GROUP BY account_type
)
SELECT
    account_type,
    min_abs_balance,
    standard_events,
    CASE WHEN account_type RLIKE '^(LOAN|CREDIT)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS account_type_zone
FROM grouped
