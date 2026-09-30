SELECT *, RANK() OVER (PARTITION BY status ORDER BY start_date DESC) AS rank_in_group FROM subscriptions QUALIFY rank_in_group = 1
