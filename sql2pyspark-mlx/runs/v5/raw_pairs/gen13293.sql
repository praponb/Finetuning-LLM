SELECT c.country, COUNT(a.id) AS account_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(a.account_type))) AS account_type_values
FROM customers c
LEFT JOIN accounts a ON a.customer_id = c.id
GROUP BY c.country
ORDER BY account_type_values;
