WITH ranked_suppliers AS (SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS row_num FROM suppliers) SELECT * FROM ranked_suppliers WHERE row_num <= 2;
