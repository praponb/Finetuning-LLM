SELECT id, customer_id, started_at, LAG(started_at, 1) OVER (PARTITION BY customer_id ORDER BY started_at, id) prev_started_at FROM sessions WHERE device NOT IN ('tablet', 'desktop');
