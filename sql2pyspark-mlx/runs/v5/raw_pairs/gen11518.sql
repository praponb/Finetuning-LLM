select end_date, customer_id from subscriptions where customer_id in (10, 19, 26, 38) union select end_date, customer_id from subscriptions where customer_id is null;
