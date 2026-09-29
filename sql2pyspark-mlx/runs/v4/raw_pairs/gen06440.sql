SELECT region, city FROM stores INTERSECT SELECT region, city FROM stores WHERE MONTH(opened_date) IN (1, 4, 8);
