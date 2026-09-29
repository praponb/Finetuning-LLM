select start_date from subscriptions where status = 'CANCELLED' intersect select start_date from subscriptions where plan != 'standard'
