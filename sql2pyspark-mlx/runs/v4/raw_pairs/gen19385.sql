WITH valid AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        ROUND(balance * 0.9, 2) AS adjusted_balance
    FROM accounts
    WHERE opened_date <= '2025-04-01'
),
with_latest AS (
    SELECT
        s.*,
        x.transaction_txn_time,
        x.merchant
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < s.opened_date
        )
),
matched AS (
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM eligible src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.opened_date >= sub.start_date
       AND src.opened_date <= DATE_ADD(sub.end_date, 10)
),
scored AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        adjusted_balance,
        transaction_txn_time,
        merchant,
        subscription_id,
        plan,
        monthly_fee,
        AVG(balance) OVER (PARTITION BY merchant ORDER BY subscription_id, id, subscription_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS balance_avg_3,
        LAST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY balance, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance
    FROM matched
),
not_flagged AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.transaction_txn_time
        )
)
SELECT
    customer_id,
    APPROX_PERCENTILE(last_known_balance, 0.9) AS last_known_balance_p90,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list
FROM not_flagged
GROUP BY ROLLUP (customer_id);
