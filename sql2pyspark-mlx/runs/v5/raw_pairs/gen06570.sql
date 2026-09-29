SELECT id, order_id, amount FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount, id) AS rnk FROM payments) AS t WHERE rnk <= 3 ORDER BY id DESC
