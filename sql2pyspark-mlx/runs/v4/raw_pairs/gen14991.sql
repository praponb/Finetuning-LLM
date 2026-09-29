SELECT *
FROM (
    SELECT CASE WHEN channel IN ('store', 'partner') THEN 'priority' ELSE 'other' END AS channel_group, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list, MAX(amount) - MIN(amount) AS amount_range
    FROM orders
    WHERE channel IS NULL
    GROUP BY CASE WHEN channel IN ('store', 'partner') THEN 'priority' ELSE 'other' END
) AS s
WHERE amount_range > 56
ORDER BY 1;
