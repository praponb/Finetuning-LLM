SELECT a.opened_date, a.customer_id, t.merchant_values
FROM accounts a
LEFT JOIN (SELECT account_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) merchant_values
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
