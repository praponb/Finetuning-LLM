WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
    FROM transactions
    WHERE merchant IS DISTINCT FROM 'Grab'
      AND amount > 2300
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        DENSE_RANK() OVER (PARTITION BY merchant ORDER BY amount ASC NULLS LAST) AS amount_dense_rank
    FROM cleaned
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        e.id,
        e.account_id,
        e.txn_type,
        e.merchant,
        e.is_flagged_label,
        e.running_amount,
        e.amount_dense_rank,
        b.balance
    FROM metrics e
    INNER JOIN accounts b
        ON e.account_id = b.id
    WHERE b.customer_id IS NOT NULL
),
with_dims_transactions AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        b.status,
        b.account_type
    FROM with_dims a
    LEFT JOIN accounts b
        ON a.account_id = b.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, txn_type ORDER BY running_amount ASC NULLS FIRST, id ASC) AS latest_rank
        FROM with_dims_transactions
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    id,
    txn_type,
    merchant,
    is_flagged_label,
    amount_dense_rank,
    balance,
    status,
    account_type,
    latest_rank
FROM latest
ORDER BY id DESC NULLS LAST;
