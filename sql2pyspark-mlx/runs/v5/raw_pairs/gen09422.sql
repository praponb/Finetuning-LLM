select start_date, 'subscription' as source from subscriptions union select opened_date, 'account' as source from accounts where opened_date < '2025-05-31'
