SELECT o.customer_id, SORT_ARRAY(COLLECT_SET(p.status)) status_list, ROUND(AVG(p.amount), 2) avg_amount
FROM payments p
JOIN orders o ON o.id = p.order_id
JOIN stores s ON o.store_id = s.id
WHERE s.opened_date < '2025-03-28'
GROUP BY o.customer_id;
