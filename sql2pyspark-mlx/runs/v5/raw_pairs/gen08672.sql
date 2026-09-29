SELECT a.customer_id, t.merchant_values
FROM accounts a
JOIN (SELECT account_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values
    FROM transactions
    WHERE txn_type = 'DEBIT'
    GROUP BY account_id) t ON a.id = t.account_id
ORDER BY a.customer_id DESC;
