SELECT * FROM (SELECT id, status, monthly_fee, end_date, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee, id) AS rnk FROM subscriptions) AS t WHERE rnk <= 3;
