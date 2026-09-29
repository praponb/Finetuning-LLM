SELECT id, location, budget FROM (SELECT id, location, budget, RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS pos FROM departments) AS t WHERE pos <= 3 ORDER BY location;
