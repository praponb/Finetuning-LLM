WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(account_type) AS account_type_upper,
        COALESCE(status, 'OTHER') AS resolved_status
    FROM accounts
    WHERE opened_date > DATE '2023-06-30'
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM valid src
    JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.opened_date >= sub.start_date
       AND src.opened_date < sub.end_date + INTERVAL '7' DAY
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id, status), 4) AS monthly_fee_share,
        SUM(subscription_id) OVER (PARTITION BY status) AS status_sum_subscription_id,
        LEAD(subscription_id, 1, 0) OVER (PARTITION BY plan, customer_id ORDER BY subscription_id, id, subscription_id) AS next_subscription_id
    FROM attributed
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.txn_type
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_type,
        balance,
        subscription_id,
        monthly_fee,
        monthly_fee_share,
        COUNT(status_sum_subscription_id) OVER (PARTITION BY account_type, customer_id ORDER BY next_subscription_id, id, subscription_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_status_sum_subscription_id,
        AVG(status_sum_subscription_id) OVER (PARTITION BY account_type, customer_id ORDER BY next_subscription_id, id, subscription_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_status_sum_subscription_id,
        SUM(status_sum_subscription_id) OVER (PARTITION BY account_type, customer_id ORDER BY next_subscription_id, id, subscription_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_status_sum_subscription_id,
        MAX(monthly_fee_share) OVER (PARTITION BY customer_id) AS customer_id_max_monthly_fee_share
    FROM with_last
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.transaction_merchant,
        top1.transaction_amount
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        account_type,
        transaction_merchant,
        last4_count_status_sum_subscription_id
    FROM latest_match
) src
PIVOT (
    MAX(last4_count_status_sum_subscription_id)
    FOR transaction_merchant IN ('7-Eleven' AS v_7_eleven, 'Starbucks' AS starbucks, 'Grab' AS grab)
)
