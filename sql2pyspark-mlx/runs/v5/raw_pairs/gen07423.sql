SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) method_values FROM payments WHERE amount BETWEEN 870 AND 1190 OR method <> 'card' GROUP BY status ORDER BY method_values, status;
