SELECT id FROM promotions EXCEPT ALL SELECT id FROM promotions WHERE promo_code <> 'BUNDLE';
