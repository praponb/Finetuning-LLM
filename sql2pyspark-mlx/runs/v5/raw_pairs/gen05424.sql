SELECT order_id, CASE WHEN status IN ('REFUNDED', 'FAILED') THEN 'Group A' ELSE 'Other' END AS status_group
FROM payments
WHERE status NOT IN ('REFUNDED', 'FAILED') AND amount < 780;
