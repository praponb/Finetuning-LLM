SELECT id, shipping_fee FROM orders WHERE shipping_fee >= (SELECT MAX(shipping_fee) FROM orders) ORDER BY 1 ASC;
