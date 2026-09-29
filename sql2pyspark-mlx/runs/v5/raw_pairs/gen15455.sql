SELECT promo_code, channel
FROM promotions
INTERSECT ALL
SELECT promo_code, channel
FROM promotions
WHERE ends_on BETWEEN '2024-08-11' AND '2025-06-19';
