SELECT * FROM customers QUALIFY ROW_NUMBER() OVER (PARTITION BY segment, country ORDER BY signup_date DESC, id) = 1;
