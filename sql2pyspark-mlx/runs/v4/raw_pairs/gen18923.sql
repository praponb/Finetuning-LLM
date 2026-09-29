WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date DESC, id) AS pos FROM products) SELECT id, category, launch_date, price FROM numbered WHERE pos = 1
