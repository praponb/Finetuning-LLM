SELECT *, RANK() OVER (PARTITION BY region, city ORDER BY sqft) AS dedup_rank FROM stores QUALIFY dedup_rank = 1;
