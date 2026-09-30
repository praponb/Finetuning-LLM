select id, start_date, status from subscriptions where plan in ('standard', 'enterprise') order by id desc
