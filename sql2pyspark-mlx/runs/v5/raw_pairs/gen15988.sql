select id, customer_id, end_date, rank() over (partition by customer_id order by end_date) as end_date_rank from subscriptions where end_date between '2023-10-01' and '2025-01-01';
