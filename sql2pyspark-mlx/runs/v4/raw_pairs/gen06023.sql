SELECT id, city, opened_date, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) rn FROM stores WHERE sqft <= 9950
