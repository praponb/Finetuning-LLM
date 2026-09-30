WITH numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY supplier_id ORDER BY cost) row_num FROM products) SELECT id, supplier_id, cost, brand FROM numbered WHERE row_num <= 5 ORDER BY cost DESC;
