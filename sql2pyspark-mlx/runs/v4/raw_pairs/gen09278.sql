SELECT COUNT(balance) balance_count, SORT_ARRAY(COLLECT_SET(account_type)) account_type_list
FROM accounts
WHERE opened_date BETWEEN '2025-01-01' AND '2025-10-01';
