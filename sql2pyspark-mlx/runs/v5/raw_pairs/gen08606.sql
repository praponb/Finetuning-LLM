WITH account_summary AS (
    SELECT account_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values, SUM(CASE WHEN merchant = 'Grab' THEN 1 ELSE 0 END) AS grab_count
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.account_type,
    a.status,
    as.merchant_values,
    as.grab_count
FROM accounts a
JOIN account_summary as ON a.id = as.account_id
WHERE as.grab_count > 49
ORDER BY merchant_values DESC;
