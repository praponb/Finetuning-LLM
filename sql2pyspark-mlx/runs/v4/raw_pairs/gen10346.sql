SELECT id, status, plan, CASE WHEN plan IN ('enterprise', 'standard') THEN 'Priority' ELSE 'Other' END AS plan_group FROM subscriptions WHERE monthly_fee < 120 OR customer_id = 20;
