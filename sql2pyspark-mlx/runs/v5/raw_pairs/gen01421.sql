SELECT id, supplier_name, city, rating FROM suppliers QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC NULLS LAST, id) = 1;
