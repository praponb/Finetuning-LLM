select quarter(end_date) as end_quarter, status, plan, count(*) as row_count from subscriptions where end_date between '2023-11-21' and '2025-05-01' group by 1, 2, 3
