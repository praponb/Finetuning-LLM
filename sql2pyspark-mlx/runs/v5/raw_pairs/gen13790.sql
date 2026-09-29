select plan, date_format(start_date, 'yyyy-MM') as start_month, count(*) as num_subscriptions from subscriptions group by 1, 2 having num_subscriptions >= 3;
