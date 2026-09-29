WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_type,
        balance,
        status,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date,
        COALESCE(status, 'OTHER') AS status_filled
    FROM accounts
    WHERE status IS NOT NULL
      AND balance <= 71000
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        balance,
        days_since_opened_date,
        status_filled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY days_since_opened_date, id) AS row_num
        FROM cleaned
    ) t
    WHERE row_num = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(days_since_opened_date AS STRING) AS days_since_opened_date_str
    FROM ranked
    WHERE balance > 73000
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.days_since_opened_date,
        latest.merchant,
        latest.transaction_txn_time
    FROM valid s
    JOIN LATERAL (
            SELECT c.merchant, c.txn_time AS transaction_txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        days_since_opened_date,
        merchant,
        transaction_txn_time
    FROM (
        SELECT *, RANK() OVER (PARTITION BY merchant ORDER BY transaction_txn_time NULLS FIRST) AS rank_in_group
        FROM with_last
    ) t
    WHERE rank_in_group = 1
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.days_since_opened_date,
        s.merchant,
        s.transaction_txn_time,
        latest.transaction_txn_time_2,
        latest.transaction_amount
    FROM latest s
    JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time_2, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.transaction_txn_time
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    days_since_opened_date,
    merchant,
    transaction_txn_time,
    transaction_txn_time_2,
    transaction_amount
FROM with_latest
ORDER BY days_since_opened_date NULLS FIRST
