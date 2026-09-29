SELECT
    account_id,
    YEAR(txn_time) txn_year,
    SORT_ARRAY(COLLECT_SET(merchant)) merchant_list,
    ROUND(AVG(amount), 2) avg_amount
FROM transactions
GROUP BY account_id, txn_year
ORDER BY avg_amount DESC
