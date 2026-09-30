WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(balance * 0.85, 2) AS balance_adjusted,
        COALESCE(status, 'N/A') AS status_clean
    FROM accounts
    WHERE account_type NOT IN ('SAVINGS', 'LOAN', 'CREDIT')
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.merchant,
        latest.txn_type
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.merchant, c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(balance_adjusted) OVER (PARTITION BY status) AS status_sum_balance_adjusted,
        AVG(balance) OVER (PARTITION BY status ORDER BY balance_adjusted, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS balance_avg_2_prior,
        LAST_VALUE(merchant) IGNORE NULLS OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant
    FROM with_latest
    WHERE merchant = 'Amazon'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(balance_avg_2_prior) OVER (ORDER BY balance_avg_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY customer_id, merchant ORDER BY balance_avg_2_prior, id) AS seq_no,
        SUM(balance) OVER (PARTITION BY status, customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY balance, id) AS row_seq
    FROM with_windows
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.transaction_merchant,
        latest.transaction_amount
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows_accounts AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(balance_avg_2_prior, 2) OVER w_recent AS prev_balance_avg_2_prior,
        ROW_NUMBER() OVER w_part AS row_seq_2
    FROM with_last
    WINDOW w_recent AS (PARTITION BY txn_type ORDER BY row_seq, id), w_part AS (ORDER BY opened_date DESC, id)
),
with_last_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.transaction_txn_type,
        top1.transaction_merchant_2
    FROM with_windows_accounts s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type, c.merchant AS transaction_merchant_2
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Join a dimension and aggregate
SELECT
    p.segment,
    s.account_type,
    COUNT(*) AS rows_total,
    AVG(s.seq_no) AS avg_seq_no
FROM with_last_accounts s
JOIN customers p ON s.customer_id = p.id
GROUP BY p.segment, s.account_type
ORDER BY avg_seq_no NULLS FIRST;
