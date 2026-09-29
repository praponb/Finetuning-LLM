SELECT *, DATE_FORMAT(order_date, 'yyyy-MM-dd') order_date_label
FROM orders
WHERE amount <= 520 AND channel IN ('store', 'web')
ORDER BY id DESC
