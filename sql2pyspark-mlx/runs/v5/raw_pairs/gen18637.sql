SELECT p.order_id, p.amount, o.channel, o.shipping_fee FROM payments p INNER JOIN orders o ON o.id = p.order_id WHERE YEAR(o.order_date) = 2024
