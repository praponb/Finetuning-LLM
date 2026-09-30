select customer_id, device, quarter(started_at) as started_at_quarter from sessions where started_at < '2025-06-10' order by started_at_quarter asc, device desc;
