SELECT id, amount, paid_at, status FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY paid_at DESC NULLS FIRST, id) AS row_num FROM payments) x WHERE row_num = 1;
