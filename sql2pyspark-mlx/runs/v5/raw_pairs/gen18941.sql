SELECT id, monthly_fee, status, CONCAT_WS(',', status, plan) AS status_plan_key FROM subscriptions ORDER BY id DESC, monthly_fee ASC;
