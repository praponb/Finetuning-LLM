WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(txn_time, 'MMM yyyy') AS txn_time_label,
        COALESCE(merchant, 'OTHER') AS resolved_merchant
    FROM transactions
    WHERE txn_time > TIMESTAMP '2023-05-18 11:00:00'
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.txn_type,
        s.amount,
        s.merchant,
        b.opened_date,
        b.balance
    FROM filtered s
    JOIN accounts b
        ON s.account_id = b.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        amount,
        merchant,
        balance,
        MAX(amount) OVER (PARTITION BY account_id) AS account_id_max_amount,
        CUME_DIST() OVER (PARTITION BY merchant ORDER BY balance DESC NULLS FIRST) AS balance_cume_dist
    FROM enriched
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY merchant, account_id ORDER BY amount ASC, id ASC) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY merchant ORDER BY amount ASC NULLS FIRST) AS amount_dense_rank,
        ROW_NUMBER() OVER (ORDER BY balance_cume_dist DESC, id ASC) AS row_seq
    FROM metrics
    WHERE merchant IN ('Starbucks', 'Grab', 'Amazon')
),
scored_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        merchant,
        balance,
        account_id_max_amount,
        balance_cume_dist,
        amount_dense_rank,
        row_seq,
        AVG(account_id_max_amount) OVER w_running AS account_id_max_amount_avg_3,
        DENSE_RANK() OVER (PARTITION BY account_id ORDER BY seq_no ASC NULLS FIRST) AS seq_no_dense_rank,
        NTILE(3) OVER w_recent AS amount_bucket
    FROM scored
    WINDOW w_running AS (ORDER BY row_seq ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY merchant ORDER BY amount ASC, id ASC)
)
SELECT
    id,
    amount_dense_rank,
    row_seq,
    account_id_max_amount_avg_3,
    seq_no_dense_rank,
    amount_bucket
FROM scored_transactions
ORDER BY id ASC NULLS LAST;
