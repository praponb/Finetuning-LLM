select *, length(plan) as plan_length from subscriptions where customer_id is not null order by monthly_fee desc
