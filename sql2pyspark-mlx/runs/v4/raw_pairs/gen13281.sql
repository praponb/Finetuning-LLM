SELECT * FROM (SELECT order_id, method, amount FROM payments) src PIVOT (MAX(amount) AS total, COUNT(1) AS item_count FOR method IN ('card' AS card, 'promptpay' AS promptpay))
