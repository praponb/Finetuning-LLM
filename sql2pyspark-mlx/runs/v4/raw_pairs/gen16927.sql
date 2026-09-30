SELECT id, customer_id, shipping_fee FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee, id) AS pos FROM orders) AS r WHERE pos <= 3 ORDER BY id DESC;
