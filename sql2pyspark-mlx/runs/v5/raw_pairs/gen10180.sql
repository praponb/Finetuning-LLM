SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee, id) AS rnk FROM orders) AS ranked WHERE rnk <= 2;
