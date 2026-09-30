select id, end_date, status from subscriptions where status in ('PAUSED', 'ACTIVE') union all select id, end_date, status from subscriptions where status in ('EXPIRED', 'CANCELLED') order by id
