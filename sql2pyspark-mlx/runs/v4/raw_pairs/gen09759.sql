WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(balance) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS balance_avg_6_prior,
        NTILE(10) OVER (PARTITION BY status ORDER BY balance, id) AS balance_bucket,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS balance_avg_2
    FROM accounts
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(balance_avg_6_prior) OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance_avg_6_prior,
        SUM(balance) OVER (PARTITION BY account_type, customer_id ORDER BY balance_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
    FROM scored
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        balance_avg_6_prior,
        balance_bucket,
        balance_avg_2,
        running_balance_avg_6_prior,
        running_balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance_avg_2, id) AS event_seq,
        SUM(running_balance_avg_6_prior) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_balance_avg_6_prior,
        FIRST_VALUE(balance_avg_2, TRUE) OVER (PARTITION BY customer_id ORDER BY running_balance_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance_avg_2
    FROM eligible
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM enriched a
    INNER JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.opened_date >= sub.start_date
       AND a.opened_date < sub.end_date + INTERVAL '30' DAY
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.transaction_txn_time,
        latest.transaction_amount
    FROM attributed s
    JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN first_known_balance_avg_2 >= 74024 THEN 'PREMIUM' ELSE 'STANDARD' END AS first_known_balance_avg_2_tier,
        CASE WHEN balance_avg_2 >= 85345 THEN 'HIGH' ELSE 'LOW' END AS balance_avg_2_tier,
        subscription_id,
        PERCENTILE_APPROX(balance_bucket, ARRAY(0.25, 0.5, 0.75)) AS balance_bucket_quartiles,
        AVG(balance) FILTER (WHERE status = 'CLOSED') AS closed_avg_balance,
        SIZE(COLLECT_SET(plan)) AS num_plans,
        SUM(CASE WHEN account_type = 'CHECKING' THEN balance ELSE 0 END) AS checking_balance,
        COUNT(CASE WHEN account_type = 'LOAN' THEN 1 END) AS loan_count,
        COUNT(*) AS cnt
    FROM with_last
    GROUP BY CUBE (CASE WHEN first_known_balance_avg_2 >= 74024 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN balance_avg_2 >= 85345 THEN 'HIGH' ELSE 'LOW' END, subscription_id)
    HAVING closed_avg_balance >= 0
)
SELECT
    first_known_balance_avg_2_tier,
    balance_avg_2_tier,
    subscription_id,
    balance_bucket_quartiles,
    closed_avg_balance,
    num_plans,
    checking_balance,
    loan_count,
    cnt
FROM totals
ORDER BY first_known_balance_avg_2_tier DESC NULLS LAST, loan_count;
