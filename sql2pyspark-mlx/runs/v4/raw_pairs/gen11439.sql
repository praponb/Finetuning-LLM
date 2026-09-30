WITH attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM accounts a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.opened_date >= sub.start_date
       AND a.opened_date <= DATE_ADD(sub.end_date, 7)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY plan, status ORDER BY opened_date DESC NULLS LAST) AS row_num
    FROM attributed
    QUALIFY row_num = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY account_type, plan ORDER BY opened_date NULLS FIRST) = 1
),
deduped_accounts AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY subscription_id NULLS LAST, id, subscription_id) = 1
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.transaction_txn_time,
        top1.transaction_amount
    FROM deduped_accounts s
    JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT customer_id, account_type
    FROM with_latest
    INTERSECT
    SELECT customer_id, account_type
    FROM accounts
    WHERE status IN ('FROZEN', 'CLOSED', 'OPEN')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.account_type = k.account_type
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.email,
        c.credit_limit,
        c.country
    FROM reconciled a
    INNER JOIN customers c
        ON a.customer_id = c.id
)
SELECT
    s.customer_id,
    s.opened_date,
    s.plan,
    s.monthly_fee,
    s.transaction_txn_time,
    s.transaction_amount,
    s.credit_limit,
    CASE WHEN plan RLIKE '^(premium|basic)$' THEN 'GROUP_B' WHEN plan RLIKE '^(standard|enterprise)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS plan_zone,
    (SELECT MIN(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM enriched s
ORDER BY opened_date NULLS LAST
