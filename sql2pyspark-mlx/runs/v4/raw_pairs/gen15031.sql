SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC, id) AS rn FROM orders) x WHERE rn = 1
