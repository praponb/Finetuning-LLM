select id, plan, monthly_fee, row_number() over (partition by plan order by monthly_fee desc, id) rn from subscriptions
