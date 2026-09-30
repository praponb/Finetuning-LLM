select id, plan, monthly_fee, min(monthly_fee) over (partition by plan) as plan_min_monthly_fee from subscriptions order by id desc;
