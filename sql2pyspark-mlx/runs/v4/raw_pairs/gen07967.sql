SELECT c.country, COUNT(*) AS row_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(a.account_type))) AS account_type_values FROM accounts a JOIN customers c ON a.customer_id = c.id GROUP BY c.country;
