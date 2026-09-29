SELECT * FROM (SELECT customer_id, status, shipping_fee FROM orders) PIVOT (AVG(shipping_fee) FOR status IN ('CANCELLED', 'PENDING'));
