SELECT *, CONCAT_WS(',', status, method) AS status_method_key
FROM payments
WHERE amount > 680
ORDER BY order_id DESC, 1 DESC;
