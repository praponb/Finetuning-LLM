SELECT
    o.discount,
    o.customer_id,
    o.shipping_fee,
    p.paid_at,
    p.amount
FROM orders o
INNER JOIN payments p ON p.order_id = o.id
WHERE p.method NOT IN ('bank_transfer', 'paypal', 'cash') OR o.order_date BETWEEN '2023-11-09' AND '2024-08-01';
