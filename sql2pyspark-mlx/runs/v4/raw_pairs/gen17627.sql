SELECT status, MIN(discount) AS min_discount, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list, COUNT(DISTINCT amount) AS unique_stores FROM orders WHERE amount BETWEEN 1110 AND 1320 GROUP BY status
