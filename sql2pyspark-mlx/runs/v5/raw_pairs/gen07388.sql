SELECT is_flagged, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
FROM transactions
WHERE txn_type IN ('FEE', 'CREDIT', 'REFUND')
GROUP BY is_flagged;
