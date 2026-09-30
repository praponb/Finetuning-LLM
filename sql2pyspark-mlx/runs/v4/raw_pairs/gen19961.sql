SELECT account_type, SORT_ARRAY(COLLECT_SET(account_type)) account_type_list, SUM(balance) total_balance FROM accounts WHERE balance <= 38000 GROUP BY 1;
