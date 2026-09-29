select start_date from subscriptions where customer_id in (9, 22) intersect select start_date from subscriptions where customer_id in (21, 22, 33, 38);
