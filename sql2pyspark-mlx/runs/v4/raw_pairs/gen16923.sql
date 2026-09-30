SELECT id, carrier, cost, delivered_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost, id) AS row_num FROM shipments) AS t WHERE row_num = 1 ORDER BY 1 DESC;
