SELECT YEAR(shipped_date) shipped_year, COUNT(cost) cost_count
FROM shipments
WHERE cost >= 45 AND cost >= 91
GROUP BY shipped_year;
