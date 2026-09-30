WITH Numbered AS (SELECT id, location, budget, RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS rn FROM departments) SELECT id, location, budget FROM Numbered WHERE rn = 1;
