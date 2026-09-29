select year(shipped_date) as shipped_year, round(avg(cost), 2) as avg_cost from shipments where cost >= 44 or status not in ('DELIVERED', 'LOST', 'RETURNED') group by year(shipped_date);
