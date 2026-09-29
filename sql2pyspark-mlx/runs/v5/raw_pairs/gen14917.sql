WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        COALESCE(balance, 0) balance_filled
    FROM accounts
    WHERE opened_date > DATE '2023-09-25'
      AND status IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(balance_filled) OVER (PARTITION BY customer_id, account_type ORDER BY opened_date, id) prev_balance_filled,
        LAG(balance) OVER (PARTITION BY customer_id, account_type ORDER BY opened_date, id) prev_balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY opened_date, id) seq_no
    FROM base
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM enriched e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.opened_date BETWEEN sub.start_date AND sub.end_date
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        subscription_id,
        plan,
        monthly_fee
    FROM attributed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, plan ORDER BY balance_filled DESC, id, subscription_id) = 1
),
enriched_accounts AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(monthly_fee) OVER w_recent next_monthly_fee,
        LAG(monthly_fee) OVER w_recent prev_monthly_fee,
        LEAD(balance) OVER w_recent next_balance,
        ROW_NUMBER() OVER w_recent seq_no
    FROM latest
    WHERE subscription_id >= 6
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY monthly_fee, id, subscription_id)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY prev_monthly_fee DESC NULLS LAST, id, subscription_id) AS dedup_rank
    FROM enriched_accounts
    QUALIFY dedup_rank = 1
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.balance,
        s.subscription_id,
        s.plan,
        s.monthly_fee,
        s.next_balance,
        x.transaction_amount,
        x.transaction_merchant
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount, c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    balance,
    subscription_id,
    monthly_fee,
    next_balance
FROM with_latest
ORDER BY next_balance DESC;
