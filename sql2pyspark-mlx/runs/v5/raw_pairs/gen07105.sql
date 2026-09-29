select carrier, avg(cost) as avg_cost, max(cost) as max_cost from shipments group by carrier having avg_cost > 54
