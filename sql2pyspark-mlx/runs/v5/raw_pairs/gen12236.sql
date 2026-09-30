select id, monthly_fee, upper(plan) as plan_upper from subscriptions order by id desc, plan_upper desc;
