SELECT c.segment, SORT_ARRAY(COLLECT_SET(a.status)) AS status_list, MIN(a.opened_date) AS min_opened_date
FROM accounts a
JOIN customers c ON c.id = a.customer_id
GROUP BY c.segment;
