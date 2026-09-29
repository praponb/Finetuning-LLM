SELECT
    rating,
    city,
    country,
    SUBSTRING(country, 1, 3) AS country_prefix,
    REGEXP_REPLACE(supplier_name, ' ', '') AS supplier_name_slug
FROM suppliers
WHERE city NOT IN ('Tokyo', 'Berlin') AND city IN ('Berlin', 'London', 'Singapore');
