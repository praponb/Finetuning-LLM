SELECT CASE WHEN status IN ('CLOSED', 'OPEN') THEN 'core' ELSE 'other' END AS status_group, SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM accounts
WHERE account_type = 'LOAN' OR status = 'FROZEN'
GROUP BY 1;
