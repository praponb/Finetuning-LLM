select id, start_date, status, customer_id from subscriptions where start_date > (select max(start_date) from subscriptions where month(end_date) in (3, 4, 9)) order by id desc, start_date desc;
