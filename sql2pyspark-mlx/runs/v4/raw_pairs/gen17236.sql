select id, customer_id, monthly_fee, max(monthly_fee) over (partition by customer_id) as customer_id_max_monthly_fee from subscriptions order by id desc, customer_id;
