SELECT id, name, country, city, segment FROM customers QUALIFY ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY credit_limit NULLS FIRST, id) = 1;
