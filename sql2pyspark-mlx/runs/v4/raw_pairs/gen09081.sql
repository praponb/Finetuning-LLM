SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY paid_at DESC NULLS FIRST, id) AS rn FROM payments) x WHERE rn = 1
