SELECT *, CASE WHEN cost >= 97 THEN 'High' WHEN cost >= 40 THEN 'Medium' ELSE 'Low' END AS cost_tier, COALESCE(cost, 0) AS cost_filled FROM shipments WHERE carrier IS NOT NULL;
