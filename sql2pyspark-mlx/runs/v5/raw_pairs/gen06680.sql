SELECT
    MONTH(txn_time) AS txn_month,
    CASE WHEN txn_type IN ('DEBIT', 'CREDIT') THEN 'tier_1' ELSE 'other' END AS txn_type_group,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    MAX(amount) - MIN(amount) AS amount_range,
    ROUND((100 * SUM(CASE WHEN merchant = 'Grab' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_grab
FROM transactions
WHERE account_id = 4
GROUP BY 1, 2
HAVING pct_grab < 64
ORDER BY amount_range DESC;
