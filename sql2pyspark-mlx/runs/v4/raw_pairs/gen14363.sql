SELECT promo_code, channel FROM promotions EXCEPT DISTINCT SELECT promo_code, channel FROM promotions WHERE promo_code IN ('SPRING10', 'VIP');
