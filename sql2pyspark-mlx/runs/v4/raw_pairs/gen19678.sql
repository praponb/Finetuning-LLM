select id, cost, rank() over (order by cost desc) as cost_rank from shipments where shipped_date between '2023-08-04' and '2025-06-01' order by cost_rank;
