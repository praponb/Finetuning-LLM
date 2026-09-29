SELECT *, ROW_NUMBER() OVER (PARTITION BY brand, supplier_id ORDER BY price, id) AS rn FROM products QUALIFY rn = 1;
