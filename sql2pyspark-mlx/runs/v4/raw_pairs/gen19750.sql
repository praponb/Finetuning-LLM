SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY status ORDER BY shipped_date) AS rnk FROM shipments) AS ranked WHERE rnk <= 2;
