SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY unit_price) AS row_num FROM order_items) AS r WHERE row_num <= 3 ORDER BY id DESC;
