SELECT
    a.customer_id,
    QUARTER(a.opened_date) AS opened_quarter,
    ROUND(AVG(t.amount), 2) AS avg_amount,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(t.txn_type))) AS txn_type_values
FROM transactions t
LEFT JOIN accounts a ON a.id = t.account_id
GROUP BY a.customer_id, QUARTER(a.opened_date)
ORDER BY opened_quarter DESC
