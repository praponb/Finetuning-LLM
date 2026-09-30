SELECT
    id,
    opened_date,
    city,
    region
FROM stores
WHERE opened_date = (SELECT MAX(opened_date) FROM stores);
