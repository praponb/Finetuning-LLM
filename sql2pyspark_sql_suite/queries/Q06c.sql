-- Q06c: Semi/anti joins and correlated subqueries
-- needs setup views: products, orders, order_items
-- @id: Q06c
SELECT p.product_id, p.sku, p.product_name
FROM products p
WHERE p.product_id NOT IN (
        SELECT oi.product_id
        FROM order_items oi
        WHERE oi.order_id IN (SELECT order_id FROM orders WHERE status <> 'CANCELLED')
      )
   OR EXISTS (
        SELECT 1 FROM order_items oi2
        WHERE oi2.product_id = p.product_id AND oi2.unit_price < p.list_price * 2
      )
ORDER BY p.product_id;
