SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY monthly_fee DESC, id ASC) AS rank_in_group FROM subscriptions QUALIFY rank_in_group = 1;
