SELECT id, customer_id, created_at, li_item.product_id, li_item.qty, li_item.promo FROM (SELECT id, customer_id, created_at, EXPLODE_OUTER(items) AS li_item FROM carts) e;
