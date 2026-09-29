WITH enriched AS (
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        NTILE(10) OVER (PARTITION BY txn_type ORDER BY amount DESC, id ASC) AS amount_bucket,
        ROW_NUMBER() OVER w_running AS seq_no,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 4) AS amount_share
    FROM transactions
    WINDOW w_running AS (PARTITION BY merchant ORDER BY txn_time DESC, id ASC)
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY merchant, txn_type ORDER BY seq_no DESC) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
filtered AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        running_amount,
        amount_bucket,
        seq_no,
        amount_share,
        rn,
        amount + amount_bucket AS amount_plus_amount_bucket,
        GREATEST(amount_bucket, running_amount) AS greatest_amount_bucket_running_amount
    FROM ranked
    WHERE txn_time > TIMESTAMP '2023-05-09 16:00:00'
),
joined AS (
    SELECT
        src.*,
        b.balance,
        b.opened_date
    FROM filtered src
    INNER JOIN accounts b
        ON src.account_id = b.id
),
scored AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        running_amount,
        amount_bucket,
        seq_no,
        amount_share,
        rn,
        amount_plus_amount_bucket,
        greatest_amount_bucket_running_amount,
        balance,
        opened_date,
        MAX(seq_no) OVER w_grp AS txn_type_max_seq_no,
        CUME_DIST() OVER (PARTITION BY txn_type ORDER BY amount DESC NULLS FIRST) AS amount_cume_dist
    FROM joined
    WINDOW w_grp AS (PARTITION BY txn_type)
)
SELECT
    id,
    account_id,
    amount,
    is_flagged,
    running_amount,
    amount_bucket,
    amount_share,
    rn,
    amount_plus_amount_bucket
FROM scored
ORDER BY running_amount ASC NULLS FIRST, rn DESC;
