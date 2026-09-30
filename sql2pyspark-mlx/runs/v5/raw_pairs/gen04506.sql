SELECT
    DATE_FORMAT(end_date, 'yyyy-MM') AS end_month,
    plan,
    SUM(CASE WHEN plan = 'premium' THEN monthly_fee ELSE 0 END) AS premium_monthly_fee,
    AVG(monthly_fee) AS avg_monthly_fee,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM subscriptions
WHERE start_date BETWEEN '2023-04-01' AND '2024-09-01'
GROUP BY DATE_FORMAT(end_date, 'yyyy-MM'), plan;
