WITH filtered_transactions AS (
    SELECT *, CASE WHEN txn_type = 'DEBIT' THEN 1 ELSE 0 END is_debit
    FROM transactions
    WHERE amount IS NULL
),
summary AS (
    SELECT is_flagged, SUM(CASE WHEN merchant = 'Amazon' THEN is_debit ELSE 0 END) amazon_is_debit
    FROM filtered_transactions
    GROUP BY is_flagged
)
SELECT is_flagged, amazon_is_debit
FROM summary
WHERE amazon_is_debit >= 77
ORDER BY amazon_is_debit DESC;
