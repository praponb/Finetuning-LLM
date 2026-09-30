WITH numbered AS (SELECT *, RANK() OVER (PARTITION BY channel ORDER BY shipping_fee DESC) AS row_num FROM orders) SELECT * FROM numbered WHERE row_num <= 1
