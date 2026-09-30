SELECT txn_type, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values FROM transactions GROUP BY 1 HAVING SUM(amount) >= 22000;
