select plan, status from subscriptions where plan = 'basic' except select plan, status from subscriptions where customer_id in (17, 33);
