SELECT SUM(CASE WHEN merchant = 'Amazon' THEN 1 ELSE 0 END) amazon_count FROM transactions WHERE HOUR(txn_time) BETWEEN 8 AND 15 ORDER BY amazon_count DESC;
