SELECT id, supplier_name, city, rating FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC NULLS LAST, id) AS row_num FROM suppliers) ranked WHERE row_num = 1;
