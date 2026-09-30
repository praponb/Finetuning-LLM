SELECT channel, store_id FROM orders WHERE status = 'SHIPPED' INTERSECT SELECT channel, store_id FROM orders WHERE channel NOT IN ('store', 'partner')
