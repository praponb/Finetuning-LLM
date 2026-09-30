SELECT id, status, paid_at, method FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS pos FROM payments) AS r WHERE pos <= 3 ORDER BY 1 DESC;
