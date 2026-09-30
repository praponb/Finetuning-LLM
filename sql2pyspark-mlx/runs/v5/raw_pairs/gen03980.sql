SELECT base.id, base.customer_id, elem.product_id, elem.qty, elem.price, elem.promo FROM carts base LATERAL VIEW EXPLODE(base.items) li AS elem;
