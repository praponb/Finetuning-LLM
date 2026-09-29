WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        CAST(is_flagged AS INT) AS is_flagged_flag
    FROM transactions
    WHERE merchant IS NOT NULL
)
-- Ranking and gap features
SELECT
    *,
    AVG(is_flagged_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS is_flagged_flag_avg_2,
    NTILE(3) OVER (PARTITION BY txn_type ORDER BY is_flagged_flag, id) AS is_flagged_flag_bucket
FROM valid;
