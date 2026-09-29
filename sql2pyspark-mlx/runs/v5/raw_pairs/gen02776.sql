WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY amount NULLS LAST, id) AS latest_rank
    FROM transactions
    QUALIFY latest_rank = 1
)
SELECT
    s.id,
    s.account_id,
    s.txn_time,
    s.merchant,
    s.latest_rank,
    CASE WHEN merchant RLIKE '^(Starbucks|Grab)$' THEN 'CORE' WHEN merchant RLIKE '^(7-Eleven|Amazon)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS merchant_zone,
    (SELECT MIN(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS min_transaction_amount
FROM latest s;
