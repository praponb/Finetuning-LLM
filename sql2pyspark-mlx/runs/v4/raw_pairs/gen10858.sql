select customer_id from subscriptions where customer_id in (4, 11, 18, 23) except select customer_id from subscriptions where start_date <= '2025-02-01' order by customer_id desc;
