SELECT * FROM (SELECT category, supplier_id, brand, price FROM products) src PIVOT (MAX(price) AS value_sum FOR brand IN ('Umbrella', 'Stark'));
