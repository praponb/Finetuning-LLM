WITH TopStores AS (SELECT *, RANK() OVER (PARTITION BY city ORDER BY sqft) AS rn FROM stores) SELECT id, city, sqft FROM TopStores WHERE rn = 1 ORDER BY id DESC;
