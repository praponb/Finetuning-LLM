SELECT
    plan,
    SUM(monthly_fee) AS total_monthly_fee,
    MAX(monthly_fee) AS max_monthly_fee,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM subscriptions
GROUP BY plan
HAVING max_monthly_fee > 90
ORDER BY total_monthly_fee DESC;
