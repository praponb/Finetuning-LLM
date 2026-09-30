WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        TRIM(account_type) AS clean_account_type,
        COALESCE(status, 'NONE') AS resolved_status
    FROM accounts
    WHERE opened_date < '2025-05-01'
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM cleaned a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.opened_date >= sub.start_date
       AND a.opened_date < sub.end_date + INTERVAL 7 DAYS
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY subscription_id DESC) AS rank_in_group
        FROM with_window
    ) x
    WHERE rank_in_group = 1
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        s.opened_date,
        s.status,
        s.resolved_status,
        s.subscription_id,
        latest.transaction_txn_time,
        latest.merchant
    FROM ranked s
    JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    customer_id,
    balance,
    opened_date,
    status,
    resolved_status,
    subscription_id,
    transaction_txn_time,
    merchant,
    CASE WHEN status RLIKE '^(CLOSED|OPEN)$' THEN 'GROUP_B' WHEN status RLIKE '^(FROZEN)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS status_zone
FROM latest_match
ORDER BY id ASC NULLS LAST, balance ASC;
