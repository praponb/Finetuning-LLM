WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        HOUR(txn_time) AS txn_time_hour
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_type IN ('REFUND', 'DEBIT', 'CREDIT')
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        b.opened_date,
        b.status,
        b.balance
    FROM filtered a
    INNER JOIN accounts b
        ON a.account_id = b.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT txn_type
    FROM enriched
    EXCEPT
    SELECT txn_type
    FROM transactions
    WHERE is_flagged = TRUE
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.txn_type = k.txn_type
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(txn_time_hour) OVER (PARTITION BY account_id) AS account_id_stddev_samp_txn_time_hour,
        AVG(amount) OVER (PARTITION BY txn_type, status) AS txn_type_avg_amount,
        MAX(amount) OVER (PARTITION BY txn_type, account_id) AS txn_type_max_amount,
        DENSE_RANK() OVER (PARTITION BY account_id, txn_type ORDER BY txn_time_hour NULLS FIRST) AS txn_time_hour_dense_rank
    FROM verified
),
enriched_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY account_id ORDER BY txn_time_hour DESC NULLS FIRST) AS txn_time_hour_rank,
        COUNT(txn_time_hour_dense_rank) OVER (PARTITION BY txn_type) AS txn_type_count_txn_time_hour_dense_rank
    FROM scored
),
enriched_transactions_2 AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        b.account_type,
        b.status AS account_status
    FROM enriched_transactions s
    INNER JOIN accounts b
        ON s.account_id = b.id
)
-- Ranking and gap features
SELECT
    *,
    COUNT(txn_time_hour_rank) OVER w_ordered AS account_type_count_txn_time_hour_rank,
    MIN(account_id_stddev_samp_txn_time_hour) OVER (PARTITION BY status) AS status_min_account_id_stddev_samp_txn_time_hour,
    MIN(txn_time_hour) OVER (PARTITION BY account_status) AS account_status_min_txn_time_hour,
    MAX(txn_type_avg_amount) OVER (PARTITION BY txn_type) AS txn_type_max_txn_type_avg_amount
FROM enriched_transactions_2
WINDOW w_ordered AS (PARTITION BY account_type)
