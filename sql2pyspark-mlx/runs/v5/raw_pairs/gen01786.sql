SELECT channel, SUM(CASE WHEN channel = 'mobile' THEN 1 ELSE 0 END) AS mobile_count, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list FROM orders GROUP BY channel;
