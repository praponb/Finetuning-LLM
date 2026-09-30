SELECT id, category, cost, RANK() OVER (PARTITION BY category ORDER BY cost DESC) rnk FROM products WHERE ARRAY_CONTAINS(tags, 'bestseller');
