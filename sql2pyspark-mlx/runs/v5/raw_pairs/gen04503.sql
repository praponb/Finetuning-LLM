SELECT * FROM (SELECT id, product_id, units, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units, id) AS rn FROM store_sales) AS t WHERE rn <= 2 ORDER BY rn DESC;
