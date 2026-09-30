WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(balance) OVER w_ordered AS cumulative_balance,
        SUM(balance) OVER (PARTITION BY account_type, customer_id) AS account_type_sum_balance,
        FIRST_VALUE(balance) IGNORE NULLS OVER w_ordered AS first_known_balance,
        SUM(balance) OVER w_grp AS running_balance
    FROM accounts
    WHERE account_type NOT IN ('SAVINGS', 'CHECKING')
    WINDOW w_ordered AS (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM metrics a
    INNER JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.opened_date BETWEEN sub.start_date AND sub.end_date
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_type,
        status,
        cumulative_balance,
        account_type_sum_balance,
        first_known_balance,
        running_balance,
        subscription_id,
        LOWER(account_type) AS account_type_lower,
        COALESCE(status, 'N/A') AS status_clean
    FROM with_window
    WHERE status IS NOT DISTINCT FROM 'OPEN'
      AND plan IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_type,
        cumulative_balance,
        subscription_id,
        account_type_lower,
        AVG(first_known_balance) OVER (PARTITION BY status, account_type ORDER BY account_type_sum_balance, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_first_known_balance,
        MAX(first_known_balance) OVER (PARTITION BY status, account_type ORDER BY account_type_sum_balance, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_first_known_balance,
        SUM(first_known_balance) OVER (PARTITION BY status, account_type ORDER BY account_type_sum_balance, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_first_known_balance,
        MIN(first_known_balance) OVER (PARTITION BY status, account_type ORDER BY account_type_sum_balance, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_first_known_balance
    FROM filtered
),
filtered_accounts AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_type,
        subscription_id,
        running_avg_first_known_balance,
        running_max_first_known_balance,
        running_sum_first_known_balance,
        running_min_first_known_balance,
        CAST(subscription_id AS DOUBLE) AS subscription_id_num
    FROM with_windows
    WHERE running_avg_first_known_balance >= 35000
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        subscription_id,
        running_max_first_known_balance,
        running_sum_first_known_balance,
        running_min_first_known_balance,
        subscription_id_num,
        SUM(subscription_id_num) OVER w_ordered AS cumulative_subscription_id_num,
        CUME_DIST() OVER (PARTITION BY account_type ORDER BY running_sum_first_known_balance DESC) AS running_sum_first_known_balance_cume_dist
    FROM filtered_accounts
    WINDOW w_ordered AS (PARTITION BY account_type ORDER BY subscription_id_num, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_type IN ('FEE', 'REFUND', 'CREDIT')
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN subscription_id_num >= 65 THEN 'LARGE' ELSE 'SMALL' END AS subscription_id_num_tier,
    CASE WHEN cumulative_subscription_id_num >= 339 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_subscription_id_num_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS cnt,
    ROUND(AVG(running_min_first_known_balance), 2) AS avg_running_min_first_known_balance,
    APPROX_PERCENTILE(running_min_first_known_balance, 0.95) AS running_min_first_known_balance_p95
FROM not_flagged
GROUP BY 1, 2;
