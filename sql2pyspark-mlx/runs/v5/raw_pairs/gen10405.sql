WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label,
        COALESCE(merchant, 'DIRECT') AS resolved_merchant
    FROM transactions
    WHERE txn_time > TIMESTAMP '2023-08-17 10:00:00'
      AND amount IS NULL
)
-- Keep one row per key (latest first)
SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC NULLS LAST, id) AS rank_in_group
FROM valid
QUALIFY rank_in_group = 1;
