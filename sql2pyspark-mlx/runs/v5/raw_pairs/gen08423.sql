WITH valid AS (
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        SUBSTRING(txn_type, 1, 2) txn_type_prefix,
        CAST(is_flagged AS INT) is_flagged_flag
    FROM transactions
    WHERE merchant IS NOT NULL
      AND merchant IS DISTINCT FROM 'Amazon'
)
SELECT
    *,
    LEAD(amount, 1) OVER (ORDER BY is_flagged_flag, id) next_amount,
    LEAD(amount, 1, 0) OVER (PARTITION BY account_id ORDER BY is_flagged_flag, id) next_amount_2,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) seq_no,
    LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY is_flagged_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_amount,
    NTILE(5) OVER (PARTITION BY account_id ORDER BY is_flagged_flag, id) is_flagged_flag_bucket
FROM valid
