SELECT id, promo_code, starts_on, ends_on FROM promotions QUALIFY ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY starts_on, id) = 1
