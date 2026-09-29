WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_time,
        merchant,
        is_flagged,
        DATE_FORMAT(txn_time, 'yyyy-MM-dd') AS txn_time_label,
        SUBSTRING(txn_type, 1, 3) AS txn_type_prefix
    FROM transactions
    WHERE merchant IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(merchant) IGNORE NULLS OVER w_recent AS first_known_merchant,
        ROW_NUMBER() OVER (PARTITION BY merchant, account_id ORDER BY txn_time, id) AS seq_no,
        ROW_NUMBER() OVER w_ordered AS event_seq
    FROM filtered
    WINDOW w_recent AS (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY account_id ORDER BY txn_time, id)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(seq_no) OVER w_recent AS running_seq_no,
        LAG(event_seq, 1) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_event_seq
    FROM enriched
    WINDOW w_recent AS (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label
    FROM with_windows
    WHERE txn_time > TO_DATE('2023-05-09')
      AND merchant IS NOT NULL
)
-- Multi-level aggregation
SELECT
    merchant,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    MIN(txn_time) AS first_seen,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles
FROM cleaned
GROUP BY merchant;
