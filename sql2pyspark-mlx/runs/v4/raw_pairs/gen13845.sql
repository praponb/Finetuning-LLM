SELECT CASE WHEN amount >= 1310 THEN 'high' WHEN amount >= 760 THEN 'mid' ELSE 'low' END AS amount_bucket, SUM(CASE WHEN channel = 'mobile' THEN amount ELSE 0 END) AS mobile_amount, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
FROM orders
GROUP BY CASE WHEN amount >= 1310 THEN 'high' WHEN amount >= 760 THEN 'mid' ELSE 'low' END
ORDER BY mobile_amount DESC, amount_bucket
LIMIT 3
