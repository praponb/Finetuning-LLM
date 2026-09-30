WITH flattened AS (SELECT id, INLINE(items) FROM carts) SELECT id, product_id, qty, price, promo FROM flattened ORDER BY qty ASC NULLS LAST;
