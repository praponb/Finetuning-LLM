SELECT customer_id, LEAST(discount, shipping_fee) AS least_discount_shipping_fee FROM orders WHERE status = 'COMPLETED' AND shipping_fee BETWEEN 12 AND 25 ORDER BY least_discount_shipping_fee DESC
