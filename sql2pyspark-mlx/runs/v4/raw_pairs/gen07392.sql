SELECT id, promo_code FROM promotions EXCEPT SELECT id, promo_code FROM promotions WHERE ends_on >= '2025-07-25';
