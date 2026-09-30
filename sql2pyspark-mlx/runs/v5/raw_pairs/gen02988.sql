SELECT signup_date, age, city, LOWER(name) AS name_lower, INITCAP(TRIM(country)) AS country_title FROM customers WHERE LENGTH(name) < 11;
