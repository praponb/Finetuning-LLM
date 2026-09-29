SELECT a.status, c.segment FROM accounts a INNER JOIN customers c ON a.customer_id = c.id WHERE c.city IN ('Bangkok', 'New York') AND a.customer_id IN (19, 28)
