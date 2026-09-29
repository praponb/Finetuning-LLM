SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating, id) AS latest_rank FROM suppliers) x WHERE latest_rank = 1;
