SELECT id, discount, shipping_fee FROM orders WHERE discount >= (SELECT AVG(discount) FROM orders WHERE shipping_fee < 33);
