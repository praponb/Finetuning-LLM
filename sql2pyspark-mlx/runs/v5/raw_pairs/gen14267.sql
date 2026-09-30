WITH metrics AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        NTILE(4) OVER w_grp AS amount_bucket,
        RANK() OVER w_hist AS amount_rank
    FROM transactions
    WINDOW w_grp AS (PARTITION BY account_id ORDER BY amount, id), w_hist AS (PARTITION BY txn_type ORDER BY amount NULLS FIRST)
),
enriched AS (
    SELECT a.*, p.status
    FROM metrics a
    LEFT JOIN accounts p
        ON a.account_id = p.id
),
scored AS (
    SELECT
        id,
        account_id,
        txn_type,
        merchant,
        is_flagged,
        amount_bucket,
        amount_rank,
        status,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY merchant ORDER BY amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        SUM(amount) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        SUM(amount) OVER (PARTITION BY status, account_id ORDER BY amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount_2,
        MAX(amount_rank) OVER (PARTITION BY account_id, merchant) AS account_id_max_amount_rank
    FROM enriched
),
metrics_transactions AS (
    SELECT
        id,
        account_id,
        txn_type,
        merchant,
        amount_bucket,
        status,
        cumulative_amount_2,
        account_id_max_amount_rank,
        CUME_DIST() OVER (PARTITION BY merchant, status ORDER BY account_id_max_amount_rank) AS account_id_max_amount_rank_cume_dist,
        ROUND(amount_rank / SUM(amount_rank) OVER (PARTITION BY account_id), 4) AS amount_rank_share,
        LEAD(amount_bucket, 2) OVER (PARTITION BY account_id, txn_type ORDER BY first_known_amount, id) AS next_amount_bucket
    FROM scored
)
SELECT
    id,
    account_id,
    txn_type,
    merchant,
    status,
    cumulative_amount_2,
    amount_rank_share,
    next_amount_bucket,
    CASE WHEN txn_type RLIKE '^(FEE|CREDIT)$' THEN 'CORE' WHEN txn_type RLIKE '^(DEBIT|REFUND)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS txn_type_zone
FROM metrics_transactions
ORDER BY status DESC NULLS LAST, next_amount_bucket;
