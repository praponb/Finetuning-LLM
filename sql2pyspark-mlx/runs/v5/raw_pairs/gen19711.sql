SELECT country
FROM suppliers
WHERE country IN ('US', 'SG', 'JP') AND (country = 'SG' OR country IN ('JP', 'DE'));
