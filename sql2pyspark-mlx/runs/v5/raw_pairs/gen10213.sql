WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        INITCAP(TRIM(merchant)) AS merchant_title
    FROM transactions
    WHERE account_id IN (8, 11)
      AND txn_time >= DATE '2023-09-02'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) AS rank_in_group
        FROM base
    ) ranked
    WHERE rank_in_group = 1
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        e.id,
        e.account_id,
        e.amount,
        e.merchant,
        e.is_flagged,
        e.merchant_title,
        d.opened_date
    FROM latest e
    LEFT JOIN accounts d
        ON e.account_id = d.id
    WHERE d.opened_date > '2025-06-01'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        NTILE(4) OVER (PARTITION BY merchant ORDER BY amount, id) AS amount_bucket,
        CUME_DIST() OVER (PARTITION BY account_id ORDER BY amount DESC NULLS LAST) AS amount_cume_dist,
        LEAD(amount, 1, 0) OVER (PARTITION BY merchant ORDER BY amount, id) AS next_amount
    FROM enriched
),
latest_transactions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount_cume_dist DESC, id) AS row_num
    FROM with_windows
    QUALIFY row_num = 1
)
SELECT
    id,
    account_id,
    next_amount
FROM latest_transactions
ORDER BY account_id DESC, next_amount DESC;
