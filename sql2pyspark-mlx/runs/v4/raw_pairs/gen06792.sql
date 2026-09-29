SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS rnk FROM subscriptions) AS r WHERE rnk = 1;
