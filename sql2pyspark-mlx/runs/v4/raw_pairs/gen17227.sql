SELECT plan, INITCAP(TRIM(plan)) AS plan_title FROM subscriptions WHERE monthly_fee <= 80 OR plan = 'standard';
