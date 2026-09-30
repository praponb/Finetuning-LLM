WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_type IN ('REFUND', 'FEE', 'CREDIT')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(amount) OVER (PARTITION BY txn_type, merchant ORDER BY txn_time, id) AS prev_amount,
        LEAD(amount) OVER (PARTITION BY txn_type, merchant ORDER BY txn_time, id) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY txn_time, id) AS seq_no
    FROM filtered
    WHERE is_flagged
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        p.account_type,
        p.status
    FROM with_windows a
    LEFT JOIN accounts p
        ON a.account_id = p.id
),
joined AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.merchant,
        a.is_flagged,
        a.prev_amount,
        a.seq_no,
        a.status,
        p2.opened_date
    FROM with_dims a
    JOIN accounts p2
        ON a.account_id = p2.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        merchant,
        prev_amount,
        seq_no,
        status,
        DENSE_RANK() OVER (PARTITION BY account_id ORDER BY seq_no DESC NULLS FIRST) AS seq_no_dense_rank,
        NTILE(3) OVER (PARTITION BY merchant, status ORDER BY seq_no, id) AS seq_no_bucket,
        SUM(prev_amount) OVER (PARTITION BY account_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_amount
    FROM joined
)
SELECT
    id,
    merchant,
    prev_amount,
    seq_no_bucket,
    running_prev_amount
FROM scored
ORDER BY prev_amount DESC NULLS LAST;
