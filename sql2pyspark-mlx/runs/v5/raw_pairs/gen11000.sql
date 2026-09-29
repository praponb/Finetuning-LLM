select 'account' as source, status from accounts where account_type = 'CREDIT' union select 'subscription' as source, status from subscriptions order by source
