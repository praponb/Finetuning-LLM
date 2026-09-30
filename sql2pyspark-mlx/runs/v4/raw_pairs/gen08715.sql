SELECT id, status, shipping_fee FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY shipping_fee DESC) AS pos FROM orders) AS t WHERE pos <= 3 ORDER BY status DESC
