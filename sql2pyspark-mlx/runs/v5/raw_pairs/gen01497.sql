SELECT id, category, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE category <> 'Books'
