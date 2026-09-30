SELECT *, RANK() OVER (PARTITION BY status ORDER BY cost) AS cost_rank, AVG(cost) OVER (ORDER BY cost, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS cost_avg_5_prior FROM shipments
