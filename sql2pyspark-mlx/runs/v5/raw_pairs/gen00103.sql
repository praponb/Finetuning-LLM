select 'store' as origin, opened_date from stores where sqft >= 14822 union select 'subscription' as origin, start_date from subscriptions order by origin
