SELECT id, currency, element FROM price_tiers LATERAL VIEW EXPLODE(discounts) e AS element;
