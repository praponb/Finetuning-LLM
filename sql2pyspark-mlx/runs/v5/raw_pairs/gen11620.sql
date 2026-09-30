select id, status, start_date, dense_rank() over (partition by status order by start_date) dense_rnk from subscriptions
