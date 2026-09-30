WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost, id) AS row_num FROM shipments) SELECT id, status, cost, shipped_date FROM numbered WHERE row_num <= 3;
