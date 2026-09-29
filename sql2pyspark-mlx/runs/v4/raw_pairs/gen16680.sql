select id, customer_id, start_date, percent_rank() over (partition by customer_id order by start_date) as start_date_pct_rank from subscriptions;
