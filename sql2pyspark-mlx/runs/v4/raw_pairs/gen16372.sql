select carrier, max(cost) as max_cost from shipments where shipped_date > '2025-01-27' group by carrier having max_cost > 30;
