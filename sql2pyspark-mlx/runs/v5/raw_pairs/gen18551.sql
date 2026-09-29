SELECT id, city, opened_date, sqft, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS rn, DENSE_RANK() OVER (ORDER BY sqft) AS dense_rnk FROM stores;
