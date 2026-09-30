SELECT id, supplier_name, rating, CAST(rating AS INT) AS rating_int, CEIL(rating) AS rating_whole FROM suppliers;
