WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) AS row_num FROM shipments) SELECT * FROM numbered WHERE row_num <= 3 ORDER BY cost DESC;
