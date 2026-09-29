select id, plan, monthly_fee, status from subscriptions qualify row_number() over (partition by plan, customer_id order by end_date asc, id asc) = 1;
