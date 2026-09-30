SELECT id, store_id, amount, shipping_fee FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY amount DESC, id) AS pos FROM orders) AS r WHERE pos <= 2
