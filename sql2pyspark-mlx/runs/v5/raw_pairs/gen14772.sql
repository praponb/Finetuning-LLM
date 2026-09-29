SELECT MAX(delivered_date) last_delivered_date, AVG(cost) avg_cost, COUNT(*) cnt FROM shipments WHERE cost < 93 ORDER BY cnt DESC
