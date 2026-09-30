SELECT c.country, MIN(s.start_date) min_start_date, SORT_ARRAY(COLLECT_SET(s.plan)) plan_list FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id GROUP BY c.country
