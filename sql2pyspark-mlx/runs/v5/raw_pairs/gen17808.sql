SELECT status, plan, LAST_DAY(end_date) AS end_date_month_end FROM subscriptions WHERE customer_id IS NOT NULL;
