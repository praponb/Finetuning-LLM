SELECT id, elem.qty, elem.price, elem.promo FROM (SELECT id, EXPLODE(items) AS elem FROM carts) e
