SELECT *, INITCAP(TRIM(carrier)) AS carrier_title
FROM shipments
ORDER BY delivered_date, id
LIMIT 10;
