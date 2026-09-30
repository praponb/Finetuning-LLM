WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        ROUND(amount * 0.9, 2) AS adjusted_amount,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label,
        COALESCE(merchant, 'UNKNOWN') AS resolved_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
      AND merchant IS NOT DISTINCT FROM 'Starbucks'
)
-- Window metrics over several different partitions
SELECT
    id,
    account_id,
    txn_type,
    is_flagged,
    adjusted_amount,
    resolved_merchant,
    AVG(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior,
    FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY adjusted_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
    RANK() OVER (PARTITION BY merchant ORDER BY amount NULLS LAST) AS amount_rank,
    AVG(adjusted_amount) OVER (ORDER BY adjusted_amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS adjusted_amount_avg_2
FROM base
