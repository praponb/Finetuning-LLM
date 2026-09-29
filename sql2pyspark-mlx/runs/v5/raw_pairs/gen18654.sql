WITH top_suppliers AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS row_num FROM suppliers) SELECT * FROM top_suppliers WHERE row_num <= 2;
