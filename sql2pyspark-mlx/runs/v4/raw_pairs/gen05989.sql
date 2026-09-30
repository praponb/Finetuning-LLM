select plan, start_date from subscriptions where end_date >= '2023-09-11' intersect select plan, start_date from subscriptions where end_date between '2025-04-01' and '2025-08-21' order by plan
