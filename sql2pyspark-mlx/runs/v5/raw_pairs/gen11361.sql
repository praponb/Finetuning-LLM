WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS(',', account_type, status) AS account_type_status_key,
        COALESCE(status, 'NONE') AS status_clean
    FROM accounts
    WHERE opened_date >= DATE '2023-10-01'
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM base s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.opened_date >= sub.start_date
       AND s.opened_date < sub.end_date + INTERVAL 7 DAYS
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_type,
        status,
        account_type_status_key,
        subscription_id,
        plan,
        monthly_fee,
        LEAD(balance, 1) OVER (PARTITION BY account_type ORDER BY monthly_fee, id, subscription_id) AS next_balance,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee, id, subscription_id) AS seq_no
    FROM with_window
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(subscription_id) OVER (PARTITION BY status, account_type ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS subscription_id_avg_2_prior,
        LAST_VALUE(seq_no, TRUE) OVER w_hist AS last_known_seq_no,
        AVG(seq_no) OVER (PARTITION BY status ORDER BY seq_no, id, subscription_id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS seq_no_avg_5,
        SUM(next_balance) OVER (ORDER BY subscription_id, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_balance
    FROM metrics
    WHERE next_balance > 61000
    WINDOW w_hist AS (PARTITION BY account_type ORDER BY subscription_id, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND NOT c.is_flagged
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN cumulative_next_balance >= 429644 THEN 'HIGH' ELSE 'LOW' END AS cumulative_next_balance_tier,
    status,
    account_type,
    APPROX_PERCENTILE(seq_no, 0.5) AS seq_no_p50,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END) AS savings_count,
    PERCENTILE_APPROX(last_known_seq_no, ARRAY(0.25, 0.5, 0.75)) AS last_known_seq_no_quartiles,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    COUNT(*) AS cnt
FROM eligible
GROUP BY 1, 2, 3
ORDER BY basic_rows;
