SELECT channel, promo_code
FROM promotions
EXCEPT
SELECT channel, promo_code
FROM promotions
WHERE ends_on >= '2023-07-30';
