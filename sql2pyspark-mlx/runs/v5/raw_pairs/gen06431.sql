SELECT id, supplier_id, cost, brand FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY cost DESC, id) AS rnk FROM products) AS t WHERE rnk <= 3;
