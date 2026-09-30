SELECT
    id,
    channel,
    order_date,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_date, id) AS order_date_row
FROM orders
ORDER BY id;
