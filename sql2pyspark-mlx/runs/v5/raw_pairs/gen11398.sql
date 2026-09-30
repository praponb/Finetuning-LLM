select id, monthly_fee, plan, customer_id from subscriptions where plan in ('basic', 'enterprise', 'premium') and customer_id is null order by id desc, 2 desc;
