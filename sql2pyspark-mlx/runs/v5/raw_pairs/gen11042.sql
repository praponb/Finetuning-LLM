SELECT status, SORT_ARRAY(COLLECT_SET(plan)) AS plan_list FROM subscriptions WHERE monthly_fee BETWEEN 130 AND 210 GROUP BY 1 ORDER BY status;
