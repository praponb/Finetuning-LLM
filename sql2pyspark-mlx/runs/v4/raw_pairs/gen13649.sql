SELECT o.channel, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.status))) AS status_values FROM payments p LEFT OUTER JOIN orders o ON p.order_id = o.id WHERE p.paid_at >= '2024-07-01' GROUP BY o.channel;
