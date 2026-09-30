SELECT id, item.product_id, item.qty, item.price, item.promo FROM carts LATERAL VIEW EXPLODE(items) t AS item;
