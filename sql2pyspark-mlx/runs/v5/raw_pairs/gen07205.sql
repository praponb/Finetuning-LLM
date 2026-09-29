WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 4) AS amount_share,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share_2,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM transactions
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, account_id
    FROM with_windows
    INTERSECT ALL
    SELECT id, account_id
    FROM transactions
    WHERE account_id IN (6, 21, 25)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN remaining_keys k ON s.id = k.id AND s.account_id = k.account_id
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        p.opened_date,
        p.status
    FROM kept_rows e
    LEFT JOIN accounts p
        ON e.account_id = p.id
),
joined AS (
    -- Attach accounts attributes
    SELECT
        src.account_id,
        src.is_flagged,
        src.amount_share,
        src.running_amount,
        src.opened_date,
        b.balance,
        b.account_type
    FROM enriched src
    LEFT JOIN accounts b
        ON src.account_id = b.id
),
enriched_transactions AS (
    -- Running and moving aggregates
    SELECT
        account_id,
        is_flagged,
        running_amount,
        account_type,
        ROUND(running_amount / SUM(running_amount) OVER (PARTITION BY account_type), 4) AS running_amount_share,
        RANK() OVER (PARTITION BY account_id ORDER BY balance DESC NULLS FIRST) AS balance_rank,
        DENSE_RANK() OVER w_grp AS balance_dense_rank
    FROM joined
    WINDOW w_grp AS (PARTITION BY account_type ORDER BY balance NULLS LAST)
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        e.account_id,
        e.running_amount,
        e.account_type,
        e.balance_rank,
        e.balance_dense_rank,
        a.account_type AS account_account_type
    FROM enriched_transactions e
    LEFT JOIN accounts a
        ON e.account_id = a.id
)
SELECT
    account_type,
    balance_rank,
    account_account_type
FROM with_dims
ORDER BY account_account_type DESC
