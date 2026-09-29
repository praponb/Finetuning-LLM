SELECT
    a.id,
    a.account_type,
    COUNT(t.id) AS transaction_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(t.merchant))) AS merchant_values
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
GROUP BY a.id, a.account_type
