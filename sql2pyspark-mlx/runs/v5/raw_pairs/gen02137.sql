SELECT id, promo_code FROM promotions INTERSECT ALL SELECT id, promo_code FROM promotions WHERE promo_code NOT IN ('BUNDLE', 'WELCOME5', 'VIP')
