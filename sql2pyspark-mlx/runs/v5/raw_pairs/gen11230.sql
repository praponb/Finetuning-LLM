WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        amount,
        merchant,
        is_flagged,
        DATE_FORMAT(txn_time, 'MMM yyyy') AS txn_time_label,
        COALESCE(merchant, 'DIRECT') AS resolved_merchant
    FROM transactions
    WHERE amount >= 2800
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        account_id,
        merchant,
        amount
    FROM valid
) src
PIVOT (
    MAX(amount)
    FOR merchant IN ('Shell' AS shell, '7-Eleven' AS v_7_eleven, 'Starbucks' AS starbucks)
);
