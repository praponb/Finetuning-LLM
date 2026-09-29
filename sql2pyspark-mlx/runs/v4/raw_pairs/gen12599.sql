WITH ranked AS (SELECT id, category, stock_qty, ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty, id) AS rn FROM products) SELECT id, category, stock_qty FROM ranked WHERE rn <= 2
