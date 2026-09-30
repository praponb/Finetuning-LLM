SELECT id, device, LENGTH(country) AS country_length
FROM sessions
ORDER BY device, id;
