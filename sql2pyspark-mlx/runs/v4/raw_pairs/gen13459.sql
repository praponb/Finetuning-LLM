SELECT channel, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list, MAX(amount) AS max_amount FROM orders GROUP BY channel HAVING MAX(amount) > 910 ORDER BY max_amount, channel LIMIT 10
