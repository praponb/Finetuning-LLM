SELECT status, customer_id FROM orders WHERE shipping_fee IS NOT NULL EXCEPT SELECT status, customer_id FROM orders WHERE shipping_fee BETWEEN 13 AND 17
