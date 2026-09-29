select 'account' as source, status from accounts where customer_id is not null union all select 'subscription' as source, status from subscriptions where year(start_date) >= 2024 order by status desc
