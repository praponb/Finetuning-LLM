SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values, COUNT(cost) AS cost_count, MIN(cost) AS min_cost FROM shipments WHERE order_id = 7;
