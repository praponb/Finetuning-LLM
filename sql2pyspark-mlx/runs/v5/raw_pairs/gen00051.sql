select id, promo_code from promotions intersect select id, promo_code from promotions where ends_on >= '2025-04-01'
