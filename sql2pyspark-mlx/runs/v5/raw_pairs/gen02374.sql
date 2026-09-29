select id, monthly_fee, start_date, initcap(trim(status)) as status_title, upper(plan) as plan_upper from subscriptions where status = 'ACTIVE' or customer_id = 38 order by start_date
