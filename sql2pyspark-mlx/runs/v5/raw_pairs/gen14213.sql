WITH valid AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        LENGTH(merchant) AS merchant_length,
        COALESCE(merchant, 'UNKNOWN') AS resolved_merchant
    FROM transactions
    WHERE txn_time > DATE '2023-08-21'
      AND merchant IS NOT NULL
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY merchant_length DESC, id ASC) AS rank_in_group
FROM valid
QUALIFY rank_in_group = 1;
