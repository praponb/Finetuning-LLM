SELECT
    accounts.id,
    accounts.balance,
    t.grab_amount,
    t.merchant_values
FROM accounts
LEFT OUTER JOIN (SELECT account_id, SUM(CASE WHEN merchant = 'Grab' THEN amount ELSE 0 END) AS grab_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values
    FROM transactions
    WHERE txn_time < '2024-08-01'
    GROUP BY account_id) t ON accounts.id = t.account_id
WHERE t.grab_amount >= 36;
