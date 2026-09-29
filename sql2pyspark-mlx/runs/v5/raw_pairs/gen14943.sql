SELECT id, status, amount, order_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) pos FROM payments) ranked WHERE pos <= 2 ORDER BY status DESC
