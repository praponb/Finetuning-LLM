SELECT status, SUM(CASE WHEN plan = 'premium' THEN monthly_fee ELSE 0 END) AS premium_monthly_fee, SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM subscriptions
GROUP BY status
HAVING SUM(CASE WHEN plan = 'premium' THEN monthly_fee ELSE 0 END) < 76
ORDER BY status, premium_monthly_fee DESC;
