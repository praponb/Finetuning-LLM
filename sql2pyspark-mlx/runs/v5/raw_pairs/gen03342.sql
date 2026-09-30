SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS pos FROM payments) AS t WHERE pos = 1
