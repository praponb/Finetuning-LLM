select * from products where array_contains(tags, 'clearance') or array_contains(tags, 'bestseller');
