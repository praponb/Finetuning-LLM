SELECT id, status, shipping_fee FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipping_fee DESC, id) AS rn FROM orders) AS r WHERE rn <= 3 ORDER BY shipping_fee DESC;
