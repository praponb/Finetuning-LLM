SELECT carrier, ROUND(AVG(cost), 2) avg_cost, SORT_ARRAY(COLLECT_SET(carrier)) carrier_list FROM shipments GROUP BY carrier HAVING SUM(CASE WHEN status = 'RETURNED' THEN cost ELSE 0 END) > 72;
