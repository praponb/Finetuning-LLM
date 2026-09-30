SELECT * FROM (SELECT carrier, COUNT(DISTINCT cost) AS unique_statuss FROM shipments GROUP BY carrier) AS s WHERE unique_statuss >= 6 ORDER BY carrier ASC
