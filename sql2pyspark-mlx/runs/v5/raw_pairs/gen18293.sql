WITH latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.transaction_txn_type,
        top1.transaction_merchant
    FROM accounts s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type, c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM latest_match s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.opened_date >= sub.start_date
       AND s.opened_date <= DATE_ADD(sub.end_date, 10)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(balance) OVER w_hist AS status_count_balance,
        PERCENT_RANK() OVER (ORDER BY balance ASC NULLS LAST) AS balance_percent_rank,
        LAST_VALUE(transaction_merchant) IGNORE NULLS OVER (PARTITION BY transaction_merchant, status ORDER BY opened_date ASC, id ASC, subscription_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_transaction_merchant
    FROM with_window
    WHERE transaction_txn_type = 'REFUND'
    WINDOW w_hist AS (PARTITION BY status)
),
latest_match_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.transaction_amount,
        top1.transaction_txn_type_2
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount, c.txn_type AS transaction_txn_type_2
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.credit_limit,
        p.name,
        p.email
    FROM latest_match_accounts a
    LEFT JOIN customers p
        ON a.customer_id <=> p.id
),
metrics_accounts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(monthly_fee) OVER (PARTITION BY transaction_txn_type_2, status ORDER BY opened_date ASC, id ASC, subscription_id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY transaction_txn_type_2, status ORDER BY opened_date ASC, id ASC, subscription_id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_monthly_fee,
        MAX(monthly_fee) OVER (PARTITION BY transaction_txn_type_2, status ORDER BY opened_date ASC, id ASC, subscription_id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_monthly_fee,
        MIN(monthly_fee) OVER (PARTITION BY transaction_txn_type_2, status ORDER BY opened_date ASC, id ASC, subscription_id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY transaction_txn_type_2 ORDER BY balance ASC, id ASC, subscription_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee
    FROM with_dims
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN subscription_id >= 9 THEN 'HIGH' ELSE 'LOW' END AS subscription_id_tier,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    APPROX_PERCENTILE(balance_percent_rank, 0.95) AS balance_percent_rank_p95,
    COUNT(CASE WHEN transaction_merchant = 'Shell' THEN 1 END) AS shell_count,
    PERCENTILE_APPROX(last4_max_monthly_fee, 0.9) AS last4_max_monthly_fee_p90
FROM metrics_accounts
GROUP BY CASE WHEN subscription_id >= 9 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', opened_date);
