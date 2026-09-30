SELECT status, SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list, MIN(opened_date) AS min_opened_date FROM accounts GROUP BY status;
