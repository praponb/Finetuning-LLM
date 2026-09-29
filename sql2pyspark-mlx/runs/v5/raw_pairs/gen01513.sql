WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'OTHER') AS merchant_clean
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-07-09 14:00:00'
      AND is_flagged
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        d.balance,
        d.account_type,
        d.opened_date
    FROM base src
    JOIN accounts d
        ON src.account_id = d.id
    WHERE d.account_type = 'CHECKING'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY account_type, txn_type ORDER BY is_flagged_flag NULLS LAST) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(amount, 1) OVER (ORDER BY txn_time, id) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS seq_no,
        LEAD(balance, 1) OVER (ORDER BY latest_rank, id) AS next_balance,
        RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) AS amount_rank
    FROM deduped
),
deduped_transactions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY seq_no DESC NULLS FIRST, id) AS row_num
    FROM metrics
    QUALIFY row_num = 1
)
SELECT
    id,
    account_id,
    amount,
    txn_time,
    is_flagged_flag,
    merchant_clean,
    balance,
    opened_date,
    seq_no
FROM deduped_transactions
ORDER BY id NULLS LAST
LIMIT 5;
