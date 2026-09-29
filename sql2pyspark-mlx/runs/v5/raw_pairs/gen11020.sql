WITH Numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS rn FROM stores) SELECT * FROM Numbered WHERE rn <= 5;
