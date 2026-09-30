SELECT
    DATE_TRUNC('MONTH', starts_on) AS activity_month,
    COUNT_IF(channel = 'store') AS store_events,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list_2
FROM promotions
GROUP BY 1
HAVING store_events >= 1
