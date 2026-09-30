SELECT id, customer_id, shipping_fee
FROM (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC) pos
    FROM orders
) ranked
WHERE pos <= 5
ORDER BY customer_id, id DESC;
