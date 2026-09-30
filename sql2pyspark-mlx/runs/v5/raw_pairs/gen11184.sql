WITH Ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS rnk FROM stores WHERE opened_date < '2023-12-28') SELECT * FROM Ranked WHERE rnk <= 5;
