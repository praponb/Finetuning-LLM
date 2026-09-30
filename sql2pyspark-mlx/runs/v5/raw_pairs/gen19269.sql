WITH valid AS (
    SELECT
        *,
        CEIL(balance) AS balance_whole,
        CONCAT(status, '/', account_type) AS status_account_type,
        COALESCE(status, 'DIRECT') AS resolved_status
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND status IS NOT DISTINCT FROM 'CLOSED'
),
eligible AS (
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < s.opened_date
        )
),
with_last AS (
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        s.opened_date,
        s.status,
        s.resolved_status,
        top1.amount
    FROM eligible s
    JOIN LATERAL (
            SELECT c.amount
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    SELECT *
    FROM with_last
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id ASC) = 1
),
with_window AS (
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM latest e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.opened_date >= sub.start_date
       AND e.opened_date < sub.end_date + INTERVAL 7 DAYS
),
scored AS (
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        status,
        resolved_status,
        subscription_id,
        plan,
        monthly_fee,
        LEAD(balance, 1) OVER (PARTITION BY account_type ORDER BY opened_date ASC, id ASC, subscription_id ASC) AS next_balance,
        CUME_DIST() OVER (PARTITION BY status, plan ORDER BY subscription_id DESC NULLS FIRST) AS subscription_id_cume_dist
    FROM with_window
    WHERE monthly_fee < 180
)
SELECT
    subscription_id,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows
FROM scored
GROUP BY subscription_id
ORDER BY subscription_id ASC
