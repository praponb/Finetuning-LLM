WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(txn_time, 'MMM yyyy') AS txn_time_label,
        DATE_TRUNC('WEEK', txn_time) AS txn_time_period
    FROM transactions
    WHERE merchant IS NOT NULL
      AND amount >= 2900
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time_period, id) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) AS seq_no,
        COUNT(amount) OVER (PARTITION BY txn_type, account_id) AS txn_type_count_amount,
        LAG(amount, 1) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_amount
    FROM ranked
),
ranked_transactions AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY rank_in_group DESC, id) = 1
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        d.opened_date,
        d.balance,
        d.status
    FROM ranked_transactions src
    LEFT JOIN accounts d
        ON src.account_id = d.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        txn_time_label,
        txn_time_period,
        rank_in_group,
        seq_no,
        txn_type_count_amount,
        prev_amount,
        opened_date,
        balance,
        status,
        PERCENT_RANK() OVER (ORDER BY seq_no DESC NULLS FIRST) AS seq_no_percent_rank,
        ROUND(txn_type_count_amount / SUM(txn_type_count_amount) OVER (PARTITION BY merchant, txn_type), 4) AS txn_type_count_amount_share
    FROM enriched
)
SELECT
    id,
    amount,
    txn_time,
    merchant,
    is_flagged,
    txn_time_label,
    rank_in_group,
    seq_no,
    txn_type_count_amount
FROM metrics
