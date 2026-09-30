SELECT o.status, o.channel, o.store_id, s.region FROM orders o INNER JOIN stores s ON s.id = o.store_id WHERE s.sqft BETWEEN 6784 AND 14604 ORDER BY channel DESC, region ASC;
