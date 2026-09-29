WITH filtered AS (
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CASE WHEN txn_type IN ('FEE', 'REFUND') THEN 'Core' ELSE 'Other' END AS txn_type_group,
        DATE_TRUNC('week', txn_time) AS txn_time_period
    FROM transactions
    WHERE merchant IS DISTINCT FROM 'Starbucks'
),
filtered_transactions AS (
    SELECT *, DATE_FORMAT(txn_time, 'yyyy-MM-dd') AS txn_time_label
    FROM filtered
    WHERE amount > 3600
),
metrics AS (
    SELECT
        *,
        AVG(amount) OVER w_ordered AS running_avg_amount,
        SUM(amount) OVER w_ordered AS running_sum_amount,
        MAX(amount) OVER w_ordered AS running_max_amount,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY merchant ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
    FROM filtered_transactions
    WHERE account_id IN (5, 34)
    WINDOW w_ordered AS (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    SELECT
        *,
        AVG(first_known_amount) OVER w_running AS first_known_amount_avg_2_prior,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        SUM(running_sum_amount) OVER (PARTITION BY merchant ORDER BY txn_time_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_sum_amount,
        LAST_VALUE(merchant) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY txn_time_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant
    FROM metrics
    WINDOW w_running AS (ORDER BY running_max_amount ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_ordered AS (ORDER BY running_sum_amount DESC, id ASC)
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY account_id, merchant ORDER BY running_max_amount DESC) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
),
with_windows AS (
    SELECT
        *,
        NTILE(4) OVER w_part AS running_sum_amount_bucket,
        MAX(first_known_amount) OVER (PARTITION BY account_id) AS account_id_max_first_known_amount,
        CUME_DIST() OVER (PARTITION BY account_id ORDER BY row_seq DESC) AS row_seq_cume_dist
    FROM ranked
    WINDOW w_part AS (PARTITION BY account_id, merchant ORDER BY running_sum_amount DESC, id ASC)
)
SELECT
    s.id,
    s.account_id,
    s.amount,
    s.txn_time,
    s.merchant,
    s.is_flagged,
    s.txn_type_group,
    s.txn_time_period,
    s.running_max_amount,
    (SELECT AVG(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS avg_transaction_amount
FROM with_windows s
ORDER BY txn_time ASC
