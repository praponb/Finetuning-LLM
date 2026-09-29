WITH top_suppliers AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS rnk FROM suppliers) SELECT * FROM top_suppliers WHERE rnk <= 1 ORDER BY id;
