SELECT
    status,
    MAX(end_date) AS last_end_date,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values,
    ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
GROUP BY status
HAVING stddev_monthly_fee >= 26
ORDER BY status_values, status;
