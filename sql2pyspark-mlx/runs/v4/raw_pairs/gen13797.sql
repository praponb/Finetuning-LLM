SELECT SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM accounts
WHERE customer_id IN (1, 11, 32)
